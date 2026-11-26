//! Trusted time for retention (docs/ledger-and-vault-plan.md §3.3).
//!
//! A retention lock that reads the wall clock can be opened by moving the
//! clock forward. Two rules hold it shut:
//!
//! - **Never backwards.** Every time handed out is later than the last one
//!   (the high-water mark), so a clock set back cannot backdate a row, and
//!   no two rows share a stamp.
//! - **Never ahead of a witness.** Once an anchor has reported an outside
//!   time (an S3 response's `Date`), "now" is at most that time plus the
//!   monotonic time elapsed since (`CLOCK_BOOTTIME`, which counts suspend
//!   and which no settable clock moves). A clock moved forward then releases
//!   nothing.
//!
//! Without a witness, "now" is the wall clock under the high-water rule,
//! and a retention is only as trustworthy as the host's clock. The
//! high-water mark is persisted with the chain (L2); here it is per
//! process.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

static HIGH_WATER: AtomicI64 = AtomicI64::new(i64::MIN);

/// The last outside time a witness reported, and when (monotonic).
static WITNESS: Mutex<Option<(i64, Instant)>> = Mutex::new(None);

fn wall_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Records an outside time `micros` (since the epoch) reported now.
pub fn witness(micros: i64) {
    *WITNESS.lock() = Some((micros, Instant::now()));
}

/// Raises the high-water mark to at least `micros` (a persisted one, read
/// at start-up).
pub fn raise_high_water(micros: i64) {
    HIGH_WATER.fetch_max(micros, Ordering::AcqRel);
}

/// The trusted time now, in microseconds since the epoch.
pub fn now_micros() -> i64 {
    let mut now = wall_micros();
    if let Some((at, when)) = *WITNESS.lock() {
        let bound = at.saturating_add(when.elapsed().min(Duration::from_secs(1 << 40)).as_micros() as i64);
        now = now.min(bound);
    }
    // Strictly later than every time handed out before: a stamp also names
    // its row (`__ledger_ts`), so two rows never share one.
    let mut previous = HIGH_WATER.load(Ordering::Acquire);
    loop {
        let next = now.max(previous.saturating_add(1));
        match HIGH_WATER.compare_exchange_weak(previous, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return next,
            Err(seen) => previous = seen,
        }
    }
}

/// Whether `inserted_at + retention` has passed by trusted time.
pub fn retention_passed(inserted_at: i64, retention_us: i64) -> bool {
    now_micros() >= inserted_at.saturating_add(retention_us)
}

/// Days since 1970-01-01 for a civil date (proleptic Gregorian).
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// An HTTP `Date` (`Wed, 26 Nov 2026 14:00:00 GMT`) as microseconds since
/// the epoch.
pub fn parse_http_date(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    let [_, day, mon, year, time, "GMT"] = parts.as_slice() else { return None };
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = MONTHS.iter().position(|x| x == mon)? as i64 + 1;
    let hms: Vec<i64> = time.split(':').map(|x| x.parse().ok()).collect::<Option<_>>()?;
    let [h, mi, se] = hms.as_slice() else { return None };
    let days = days_from_civil(year.parse().ok()?, m, day.parse().ok()?);
    Some(((days * 86_400) + h * 3600 + mi * 60 + se) * 1_000_000)
}

/// Trusted now plus `days`, as RFC 3339 UTC (an Object Lock retain-until).
pub fn rfc3339_days_from_now(days: i64) -> String {
    let secs = now_micros() / 1_000_000 + days * 86_400;
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_dates_parse() {
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:01 GMT"), Some(1_000_000));
        assert_eq!(parse_http_date("Wed, 26 Nov 2026 14:00:00 GMT").map(|m| m / 1_000_000), Some(1_795_701_600));
        assert!(rfc3339_days_from_now(1).ends_with('Z'));
    }

    #[test]
    fn time_never_goes_backwards() {
        let a = now_micros();
        raise_high_water(a + 10_000_000);
        assert!(now_micros() >= a + 10_000_000);
    }

    #[test]
    fn a_witness_caps_a_clock_moved_forward() {
        // A witness an hour behind the wall clock: retention measured from a
        // row inserted at the witness time does not see the extra hour.
        let wall = wall_micros();
        witness(wall - 3_600_000_000);
        assert!(!retention_passed(wall - 3_600_000_000, 1_800_000_000));
        *WITNESS.lock() = None;
    }
}
