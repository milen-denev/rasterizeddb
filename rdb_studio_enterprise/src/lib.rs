//! RasterizedDB Studio's LEDGER page (licensed: see LICENSE). It covers the
//! ledger and vault features (docs/ledger-and-vault-plan.md):
//!
//! - **Chain and tables:** chain status, anchors, each ledger table's
//!   policy, VERIFY (all or one table), receipts, raising retention, legal
//!   holds, adding anchors, acknowledging a restore's rollback.
//! - **Creating:** ledger tables and vaults.
//! - **Each vault:** its secrets (names and states; a value only on an
//!   explicit Reveal, shown until Hide or the next load), versions, set,
//!   disable and enable, delete and recover, purge, key rotation, the audit
//!   trail, and keys used in place (create, encrypt, decrypt, sign, verify,
//!   public key).
//!
//! Studio's GPL side holds the tab and runs statements. This crate builds
//! every statement, reads the answers into a [`LedgerPage`], and draws it
//! ([`view`]). Studio sends two generic messages back: run this statement
//! ([`Actions::run`]) and an input changed ([`Actions::input`]). The server
//! decides who may do what; a refusal is shown as the answer.

use std::collections::BTreeMap;

use iced::widget::{button, column, container, row, scrollable, text, text_input};
use iced::{Element, Fill};

/// Every statement the page runs to load.
pub const SHOW_SQL: &str = "SHOW LEDGER";
pub const VERIFY_SQL: &str = "VERIFY LEDGER";

/// A vault's `LIST`.
pub fn list_sql(vault: &str) -> String {
    format!("VAULT {vault} LIST")
}

/// A vault's `AUDIT`.
pub fn audit_sql(vault: &str) -> String {
    format!("VAULT {vault} AUDIT")
}

/// `s` as a SQL string literal.
pub fn q(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// One vault on the page.
#[derive(Clone, Debug, Default)]
pub struct VaultView {
    pub name: String,
    /// `LIST`'s lines: name, version, state, type, expiry. Never a value.
    pub secrets: Vec<String>,
    pub audit: Option<Vec<String>>,
}

/// What the page shows.
#[derive(Clone, Debug, Default)]
pub struct LedgerPage {
    pub show: Vec<String>,
    pub verify: Option<Vec<String>>,
    pub vaults: Vec<VaultView>,
    /// Why the page could not load: no ledger support on the server, or no
    /// permission.
    pub error: Option<String>,
    /// The page's text inputs, by key.
    pub inputs: BTreeMap<String, String>,
    /// The last action and its answer.
    pub last: Option<(String, Result<Vec<String>, String>)>,
    /// A secret revealed on request: (vault, name, value). Cleared by Hide
    /// and by every load.
    pub revealed: Option<(String, String, String)>,
}

/// The verb of a statement, for the answer's heading: never the statement,
/// which can carry a secret.
fn label(sql: &str) -> String {
    let w: Vec<&str> = sql.split_whitespace().take(3).collect();
    w.join(" ")
}

impl LedgerPage {
    /// The vaults `SHOW LEDGER` names (its lines `<name>: vault id …`).
    pub fn vault_names(show: &[String]) -> Vec<String> {
        Self::named(show, "vault ")
    }

    /// The ledger tables `SHOW LEDGER` names.
    pub fn ledger_names(show: &[String]) -> Vec<String> {
        Self::named(show, "ledger ")
    }

    fn named(show: &[String], kind: &str) -> Vec<String> {
        show.iter()
            .filter_map(|l| {
                let (name, rest) = l.split_once(": ")?;
                rest.starts_with(kind).then(|| name.to_string())
            })
            .collect()
    }

    /// Whether the last `VERIFY` found everything intact.
    pub fn verified_intact(&self) -> Option<bool> {
        self.verify.as_ref().map(|v| !v.iter().any(|l| l.starts_with("TAMPERED") || l.starts_with("UNKNOWN")))
    }

    /// Keeps what a reload must not lose: the inputs and the last answer.
    /// A revealed secret is dropped.
    pub fn carry_over(&mut self, old: LedgerPage) {
        self.inputs = old.inputs;
        self.last = old.last;
        self.revealed = None;
    }

    /// Records an action's answer. A `GET SECRET` is shown as a revealed
    /// value, never kept as an answer line.
    pub fn ran(&mut self, sql: &str, result: Result<Vec<String>, String>) {
        let w: Vec<&str> = sql.split_whitespace().collect();
        if w.len() >= 5 && w[0].eq_ignore_ascii_case("VAULT") && w[2].eq_ignore_ascii_case("GET") && w[3].eq_ignore_ascii_case("SECRET") {
            if let Ok(lines) = &result {
                let name = w[4].trim_matches('\'').to_string();
                self.revealed = Some((w[1].to_string(), name, lines.join("\n")));
                self.last = Some((label(sql), Ok(vec!["revealed below".to_string()])));
                return;
            }
        }
        // A value written by a form is not kept in the inputs once sent.
        self.inputs.retain(|k, _| !k.ends_with(":value") && !k.ends_with(":plain"));
        self.last = Some((label(sql), result));
    }

    /// Hides a revealed secret.
    pub fn hide(&mut self) {
        self.revealed = None;
    }

    fn input(&self, key: &str) -> &str {
        self.inputs.get(key).map(String::as_str).unwrap_or("")
    }
}

/// The messages the page sends back to Studio.
#[derive(Clone)]
pub struct Actions<M> {
    pub reload: M,
    pub verify: M,
    /// Load one vault's audit trail.
    pub audit: fn(String) -> M,
    /// Run a statement this page built; its answer comes back to
    /// [`LedgerPage::ran`] and the page reloads.
    pub run: fn(String) -> M,
    /// An input changed: (key, value). The key `__hide` hides a revealed secret.
    pub input: fn(String, String) -> M,
}

fn line<'a, M: 'a>(l: &str) -> Element<'a, M> {
    let bad = l.starts_with("TAMPERED") || l.starts_with("UNKNOWN") || l.contains("UNCHAINED");
    let warn = l.starts_with("WARNING");
    let t = text(l.to_string()).size(12).font(iced::Font::MONOSPACE);
    let t = if bad {
        t.color(iced::Color::from_rgb(0.9, 0.3, 0.3))
    } else if warn {
        t.color(iced::Color::from_rgb(0.9, 0.7, 0.2))
    } else {
        t
    };
    t.into()
}

fn section<'a, M: 'a>(title: String, body: Element<'a, M>) -> Element<'a, M> {
    column![text(title).size(13), container(body).padding(8).width(Fill)].spacing(4).into()
}

fn act<'a, M: Clone + 'a>(label: &str, msg: Option<M>) -> Element<'a, M> {
    let b = button(text(label.to_string()).size(12));
    match msg {
        Some(m) => b.on_press(m).into(),
        None => b.into(),
    }
}

/// A text input bound to `key`.
fn field<'a, M: Clone + 'a>(page: &'a LedgerPage, a: &Actions<M>, key: String, hint: &str, secret: bool) -> Element<'a, M> {
    let input = a.input;
    let k = key.clone();
    text_input(hint, page.input(&key))
        .secure(secret)
        .size(12)
        .width(180)
        .on_input(move |v| input(k.clone(), v))
        .into()
}

/// `Some(run(sql))` when every input it needs is filled.
fn when<M>(a: &Actions<M>, ready: bool, sql: impl FnOnce() -> String) -> Option<M> {
    ready.then(|| (a.run)(sql()))
}

/// The page.
pub fn view<'a, M: Clone + 'a>(page: Option<&'a LedgerPage>, a: Actions<M>) -> Element<'a, M> {
    let bar = row![act("RELOAD", Some(a.reload.clone())), act("VERIFY LEDGER", Some(a.verify.clone()))].spacing(8);
    let Some(page) = page else {
        return column![bar, text("Loading…").size(13)].spacing(10).into();
    };
    if let Some(e) = &page.error {
        return column![bar, text(e.as_str()).size(13)].spacing(10).into();
    }
    let mut body = column![bar].spacing(12);

    // The last answer, and a revealed secret.
    if let Some((what, result)) = &page.last {
        let lines: Vec<Element<'a, M>> = match result {
            Ok(l) if l.is_empty() => vec![line("done")],
            Ok(l) => l.iter().map(|x| line(x)).collect(),
            Err(e) => vec![line(&format!("REFUSED   {e}"))],
        };
        body = body.push(section(format!("LAST: {what}"), column(lines).spacing(2).into()));
    }
    if let Some((vault, name, value)) = &page.revealed {
        body = body.push(section(
            format!("SECRET {vault} / {name}"),
            column![
                text(value.as_str()).size(13).font(iced::Font::MONOSPACE),
                act("HIDE", Some((a.input)("__hide".to_string(), String::new()))),
            ]
            .spacing(6)
            .into(),
        ));
    }

    body = body.push(section("CHAIN AND TABLES".to_string(), column(page.show.iter().map(|l| line(l))).spacing(2).into()));
    if let Some(v) = &page.verify {
        let title = if page.verified_intact() == Some(true) { "VERIFY: INTACT" } else { "VERIFY: PROBLEMS FOUND" };
        body = body.push(section(title.to_string(), column(v.iter().map(|l| line(l))).spacing(2).into()));
    }

    // Each ledger table's actions.
    for t in LedgerPage::ledger_names(&page.show) {
        let k = |f: &str| format!("ledger:{t}:{f}");
        let retention = page.input(&k("retention")).to_string();
        let hold = page.input(&k("hold")).to_string();
        let anchor = page.input(&k("anchor")).to_string();
        let stamp = page.input(&k("stamp")).to_string();
        let tt = t.clone();
        let c = column![
            row![
                act("VERIFY", Some((a.run)(format!("VERIFY LEDGER {t}")))),
                field(page, &a, k("retention"), "e.g. 10 YEARS", false),
                act("RAISE RETENTION", when(&a, !retention.trim().is_empty(), || format!("ALTER LEDGER TABLE {tt} SET RETENTION {retention}"))),
            ]
            .spacing(6),
            row![
                field(page, &a, k("hold"), "hold name", false),
                act("PLACE HOLD", when(&a, !hold.is_empty(), || format!("ALTER LEDGER TABLE {t} SET LEGAL HOLD {}", q(&hold)))),
                act("RELEASE HOLD", when(&a, !hold.is_empty(), || format!("ALTER LEDGER TABLE {t} RELEASE LEGAL HOLD {}", q(&hold)))),
            ]
            .spacing(6),
            row![
                field(page, &a, k("anchor"), "dir:/path or s3://bucket/prefix", false),
                act("ADD ANCHOR", when(&a, !anchor.is_empty(), || format!("ALTER LEDGER TABLE {t} ADD ANCHOR {}", q(&anchor)))),
            ]
            .spacing(6),
            row![
                field(page, &a, k("stamp"), "row __ledger_ts", false),
                act("RECEIPT", when(&a, stamp.trim().parse::<i64>().is_ok(), || format!("SHOW LEDGER RECEIPT {t} {}", stamp.trim()))),
            ]
            .spacing(6),
        ]
        .spacing(6);
        body = body.push(section(format!("LEDGER TABLE {t}"), c.into()));
    }

    // Each vault.
    for vault in &page.vaults {
        let v = vault.name.clone();
        let k = |f: &str| format!("vault:{v}:{f}");
        let mut c = column![].spacing(4);
        if vault.secrets.is_empty() {
            c = c.push(text("no secrets").size(12));
        }
        for s in &vault.secrets {
            let name = s.split_whitespace().next().unwrap_or("").to_string();
            let is_key = name.starts_with("key:");
            let mut r = row![line(s)].spacing(6);
            if !is_key {
                r = r.push(act("REVEAL", Some((a.run)(format!("VAULT {v} GET SECRET {}", q(&name))))));
                r = r.push(act("VERSIONS", Some((a.run)(format!("VAULT {v} VERSIONS SECRET {}", q(&name))))));
            }
            for verb in ["DISABLE", "ENABLE", "DELETE", "RECOVER", "PURGE"] {
                r = r.push(act(verb, Some((a.run)(format!("VAULT {v} {verb} SECRET {}", q(&name))))));
            }
            c = c.push(r);
        }
        // Set a secret.
        let (sn, sv, se, st) = (page.input(&k("name")).to_string(), page.input(&k("value")).to_string(), page.input(&k("expires")).to_string(), page.input(&k("type")).to_string());
        let vv = v.clone();
        c = c.push(
            row![
                field(page, &a, k("name"), "secret name", false),
                field(page, &a, k("value"), "value", true),
                field(page, &a, k("expires"), "expires (YYYY-MM-DD)", false),
                field(page, &a, k("type"), "content type", false),
                act("SET", when(&a, !sn.is_empty() && !sv.is_empty(), move || {
                    let mut sql = format!("VAULT {vv} SET SECRET {} = {}", q(&sn), q(&sv));
                    if !se.is_empty() {
                        sql.push_str(&format!(" EXPIRES {}", q(&se)));
                    }
                    if !st.is_empty() {
                        sql.push_str(&format!(" CONTENT TYPE {}", q(&st)));
                    }
                    sql
                })),
            ]
            .spacing(6),
        );
        // Keys used in place.
        let (kn, kt, kp, ks) = (page.input(&k("key")).to_string(), page.input(&k("keytype")).to_string(), page.input(&k("plain")).to_string(), page.input(&k("sig")).to_string());
        let kind = if kt.is_empty() { "aes256-gcm".to_string() } else { kt.clone() };
        c = c.push(
            row![
                field(page, &a, k("key"), "key name", false),
                field(page, &a, k("keytype"), "aes256-gcm | mldsa65", false),
                act("CREATE KEY", when(&a, !kn.is_empty(), || format!("VAULT {v} CREATE KEY {} TYPE {}", q(&kn), q(&kind)))),
                act("PUBLIC KEY", when(&a, !kn.is_empty(), || format!("VAULT {v} PUBLIC KEY {}", q(&kn)))),
            ]
            .spacing(6),
        );
        c = c.push(
            row![
                field(page, &a, k("plain"), "text / ciphertext / message", true),
                field(page, &a, k("sig"), "signature (VERIFY)", false),
                act("ENCRYPT", when(&a, !kn.is_empty() && !kp.is_empty(), || format!("VAULT {v} ENCRYPT KEY {} VALUE {}", q(&kn), q(&kp)))),
                act("DECRYPT", when(&a, !kn.is_empty() && !kp.is_empty(), || format!("VAULT {v} DECRYPT KEY {} VALUE {}", q(&kn), q(&kp)))),
                act("SIGN", when(&a, !kn.is_empty() && !kp.is_empty(), || format!("VAULT {v} SIGN KEY {} VALUE {}", q(&kn), q(&kp)))),
                act("VERIFY", when(&a, !kn.is_empty() && !kp.is_empty() && !ks.is_empty(), || {
                    format!("VAULT {v} VERIFY KEY {} VALUE {} SIGNATURE {}", q(&kn), q(&kp), q(&ks))
                })),
            ]
            .spacing(6),
        );
        // Key rotation and the audit.
        let to = page.input(&k("rotate")).to_string();
        c = c.push(
            row![
                field(page, &a, k("rotate"), "new provider (empty: same)", false),
                act("ROTATE KEY", Some((a.run)(if to.is_empty() { format!("VAULT {v} ROTATE KEY") } else { format!("VAULT {v} ROTATE KEY TO {}", q(&to)) }))),
                act("AUDIT TRAIL", Some((a.audit)(v.clone()))),
            ]
            .spacing(6),
        );
        if let Some(audit) = &vault.audit {
            for l in audit {
                c = c.push(line(l));
            }
        }
        body = body.push(section(format!("VAULT {}", vault.name), c.into()));
    }

    // Creating ledger tables and vaults; acknowledging a rollback.
    let (ln, lc, lr) = (page.input("new:ledger:name").to_string(), page.input("new:ledger:columns").to_string(), page.input("new:ledger:retention").to_string());
    let (vn, vp, vr) = (page.input("new:vault:name").to_string(), page.input("new:vault:provider").to_string(), page.input("new:vault:retain").to_string());
    let reason = page.input("ack:reason").to_string();
    let create = column![
        row![
            field(page, &a, "new:ledger:name".into(), "table name", false),
            field(page, &a, "new:ledger:columns".into(), "id BIGINT, amount BIGINT", false),
            field(page, &a, "new:ledger:retention".into(), "7 YEARS", false),
            act("CREATE LEDGER TABLE", when(&a, !ln.is_empty() && !lc.is_empty() && !lr.is_empty(), || {
                format!("CREATE LEDGER TABLE {ln} ({lc}) NO DELETE UNTIL {lr} AFTER INSERT")
            })),
        ]
        .spacing(6),
        row![
            field(page, &a, "new:vault:name".into(), "vault name", false),
            field(page, &a, "new:vault:provider".into(), "pkcs11:… | mlkem:… | file:…", false),
            field(page, &a, "new:vault:retain".into(), "90 DAYS", false),
            act("CREATE VAULT", when(&a, !vn.is_empty() && !vp.is_empty(), || {
                let retain = if vr.is_empty() { String::new() } else { format!(" RETAIN DELETED {vr}") };
                format!("CREATE VAULT {vn} KEY PROVIDER {}{retain}", q(&vp))
            })),
        ]
        .spacing(6),
        row![
            field(page, &a, "ack:reason".into(), "why the ledger was rolled back", false),
            act("ACKNOWLEDGE ROLLBACK", when(&a, !reason.trim().is_empty(), || format!("ACKNOWLEDGE LEDGER ROLLBACK {}", q(&reason)))),
        ]
        .spacing(6),
    ]
    .spacing(6);
    body = body.push(section("CREATE / RESTORE".to_string(), create.into()));

    scrollable(body).height(Fill).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_and_vaults_are_read_from_show_ledger() {
        let show = vec![
            "chain: 3 entries, head ab, at /x".to_string(),
            "payments: ledger id 01, retention 1 s".to_string(),
            "app_secrets: vault id 02, retention 7776000 s".to_string(),
        ];
        assert_eq!(LedgerPage::vault_names(&show), vec!["app_secrets".to_string()]);
        assert_eq!(LedgerPage::ledger_names(&show), vec!["payments".to_string()]);
    }

    #[test]
    fn a_get_is_revealed_not_kept_and_set_values_are_forgotten() {
        let mut p = LedgerPage::default();
        p.inputs.insert("vault:v:value".into(), "hunter2".into());
        p.ran("VAULT v SET SECRET 'db' = 'hunter2'", Ok(vec!["db version 1".into()]));
        assert!(!p.inputs.contains_key("vault:v:value"));
        assert_eq!(p.last.as_ref().unwrap().0, "VAULT v SET", "the heading never carries the statement");
        p.ran("VAULT v GET SECRET 'db'", Ok(vec!["hunter2".into()]));
        assert_eq!(p.revealed.as_ref().unwrap().2, "hunter2");
        assert!(!format!("{:?}", p.last).contains("hunter2"));
        let mut reloaded = LedgerPage::default();
        reloaded.carry_over(p);
        assert!(reloaded.revealed.is_none());
    }

    #[test]
    fn quoting() {
        assert_eq!(q("it's"), "'it''s'");
    }
}
