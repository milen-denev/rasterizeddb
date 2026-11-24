//! Per-core counters at no hot-path cost (§4.9 G10).
//!
//! Each core counts in plain integers it alone touches (a `Cell` in its
//! `Core`). About once a second it publishes a copy to the [`Board`], which
//! `SHOW GATEWAY` and the metrics endpoint sum. Numbers read there are at
//! most a second old.

use std::sync::Mutex;

/// Hop-latency buckets: `[0, 1)`, `[1, 2)`, `[2, 4)` … µs, the last open.
pub const BUCKETS: usize = 24;

#[derive(Clone, Debug, Default)]
pub struct Counters {
    pub statements: u64,
    pub routed: u64,
    pub fan_outs: u64,
    pub template_hits: u64,
    pub template_misses: u64,
    pub template_refuted: u64,
    pub classify_errors: u64,
    pub retries: u64,
    pub lost_08007: u64,
    pub down_08006: u64,
    pub refused_no_key: u64,
    pub refused_cross_shard: u64,
    pub refusals_other: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub sessions_opened: u64,
    pub sessions_closed: u64,
    pub virtual_sessions: u64,
    pub auth_failures: u64,
    pub panics: u64,
    pub map_refreshes: u64,
    pub hop_us: [u64; BUCKETS],
}

impl Counters {
    pub fn hop(&mut self, micros: u64) {
        let b = if micros == 0 { 0 } else { (64 - micros.leading_zeros() as usize).min(BUCKETS - 1) };
        self.hop_us[b] += 1;
    }

    fn add(&mut self, o: &Counters) {
        macro_rules! sum {
            ($($f:ident),*) => {$( self.$f += o.$f; )*};
        }
        sum!(
            statements, routed, fan_outs, template_hits, template_misses, template_refuted, classify_errors, retries,
            lost_08007, down_08006, refused_no_key, refused_cross_shard, refusals_other, bytes_in, bytes_out,
            sessions_opened, sessions_closed, virtual_sessions, auth_failures, panics, map_refreshes
        );
        for i in 0..BUCKETS {
            self.hop_us[i] += o.hop_us[i];
        }
    }

    /// The hop latency at quantile `q` (0..1), as the upper edge of its
    /// bucket in µs.
    pub fn hop_quantile(&self, q: f64) -> u64 {
        let total: u64 = self.hop_us.iter().sum();
        if total == 0 {
            return 0;
        }
        let want = (total as f64 * q).ceil() as u64;
        let mut seen = 0;
        for (i, n) in self.hop_us.iter().enumerate() {
            seen += n;
            if seen >= want {
                return 1u64 << i;
            }
        }
        1u64 << (BUCKETS - 1)
    }

    /// `(name, value)` rows for `SHOW GATEWAY` and the metrics endpoint.
    pub fn rows(&self) -> Vec<(&'static str, String)> {
        vec![
            ("statements", self.statements.to_string()),
            ("routed", self.routed.to_string()),
            ("fan_outs", self.fan_outs.to_string()),
            ("template_hits", self.template_hits.to_string()),
            ("template_misses", self.template_misses.to_string()),
            ("route_template_refuted", self.template_refuted.to_string()),
            ("classify_errors", self.classify_errors.to_string()),
            ("retries", self.retries.to_string()),
            ("lost_08007", self.lost_08007.to_string()),
            ("shard_down_08006", self.down_08006.to_string()),
            ("refused_no_route_key", self.refused_no_key.to_string()),
            ("refused_cross_shard", self.refused_cross_shard.to_string()),
            ("refusals_other", self.refusals_other.to_string()),
            ("bytes_in", self.bytes_in.to_string()),
            ("bytes_out", self.bytes_out.to_string()),
            ("sessions_opened", self.sessions_opened.to_string()),
            ("sessions_open", self.sessions_opened.saturating_sub(self.sessions_closed).to_string()),
            ("virtual_sessions_opened", self.virtual_sessions.to_string()),
            ("auth_failures", self.auth_failures.to_string()),
            ("session_panics", self.panics.to_string()),
            ("map_refreshes", self.map_refreshes.to_string()),
            ("hop_p50_us", self.hop_quantile(0.5).to_string()),
            ("hop_p99_us", self.hop_quantile(0.99).to_string()),
        ]
    }
}

/// Where cores publish their counters.
pub struct Board {
    cores: Mutex<Vec<Counters>>,
}

impl Board {
    pub fn new(cores: usize) -> Board {
        Board { cores: Mutex::new(vec![Counters::default(); cores]) }
    }

    pub fn publish(&self, core: usize, c: &Counters) {
        if let Ok(mut v) = self.cores.lock()
            && let Some(slot) = v.get_mut(core)
        {
            slot.clone_from(c);
        }
    }

    pub fn sum(&self) -> Counters {
        let mut total = Counters::default();
        if let Ok(v) = self.cores.lock() {
            for c in v.iter() {
                total.add(c);
            }
        }
        total
    }
}
