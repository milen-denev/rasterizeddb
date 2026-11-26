//! RasterizedDB Studio's LEDGER page (licensed: see LICENSE): the ledger's
//! chain and anchors, each ledger table's policy, a one-click
//! `VERIFY LEDGER`, and each vault's secrets (names and states only, never
//! values) with its audit trail.
//!
//! Studio's GPL side holds the tab and runs the statements; this crate
//! decides which statements, reads their answers into a [`LedgerPage`], and
//! draws it ([`view`]).

use iced::widget::{button, column, container, row, scrollable, text};
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
}

impl LedgerPage {
    /// The vaults `SHOW LEDGER` names (its lines `<name>: vault id …`).
    pub fn vault_names(show: &[String]) -> Vec<String> {
        show.iter()
            .filter_map(|l| {
                let (name, rest) = l.split_once(": ")?;
                rest.starts_with("vault ").then(|| name.to_string())
            })
            .collect()
    }

    /// Whether the last `VERIFY` found everything intact.
    pub fn verified_intact(&self) -> Option<bool> {
        self.verify.as_ref().map(|v| !v.iter().any(|l| l.starts_with("TAMPERED") || l.starts_with("UNKNOWN")))
    }
}

/// The messages the page sends back to Studio.
#[derive(Clone)]
pub struct Actions<M> {
    pub reload: M,
    pub verify: M,
    /// Load one vault's audit trail.
    pub audit: fn(String) -> M,
}

fn line<'a, M: 'a>(l: &'a str) -> Element<'a, M> {
    let bad = l.starts_with("TAMPERED") || l.starts_with("UNKNOWN") || l.contains("UNCHAINED");
    let warn = l.starts_with("WARNING");
    let t = text(l).size(12).font(iced::Font::MONOSPACE);
    let t = if bad {
        t.color(iced::Color::from_rgb(0.9, 0.3, 0.3))
    } else if warn {
        t.color(iced::Color::from_rgb(0.9, 0.7, 0.2))
    } else {
        t
    };
    t.into()
}

fn section<'a, M: 'a>(title: &'a str, body: Element<'a, M>) -> Element<'a, M> {
    column![text(title).size(13), container(body).padding(8).width(Fill)].spacing(4).into()
}

/// The page.
pub fn view<'a, M: Clone + 'a>(page: Option<&'a LedgerPage>, actions: Actions<M>) -> Element<'a, M> {
    let bar = row![
        button(text("RELOAD").size(12)).on_press(actions.reload.clone()),
        button(text("VERIFY LEDGER").size(12)).on_press(actions.verify.clone()),
    ]
    .spacing(8);
    let Some(page) = page else {
        return column![bar, text("Loading…").size(13)].spacing(10).into();
    };
    if let Some(e) = &page.error {
        return column![bar, text(e.as_str()).size(13)].spacing(10).into();
    }
    let mut body = column![bar].spacing(12);
    body = body.push(section("CHAIN AND TABLES", column(page.show.iter().map(|l| line(l))).spacing(2).into()));
    if let Some(v) = &page.verify {
        let title = if page.verified_intact() == Some(true) { "VERIFY: INTACT" } else { "VERIFY: PROBLEMS FOUND" };
        body = body.push(section(title, column(v.iter().map(|l| line(l))).spacing(2).into()));
    }
    for vault in &page.vaults {
        let mut c = column(vault.secrets.iter().map(|l| line(l))).spacing(2);
        if vault.secrets.is_empty() {
            c = c.push(text("no secrets").size(12));
        }
        c = c.push(button(text("AUDIT TRAIL").size(12)).on_press((actions.audit)(vault.name.clone())));
        if let Some(a) = &vault.audit {
            for l in a {
                c = c.push(line(l));
            }
        }
        body = body.push(section(&vault.name, c.into()));
    }
    scrollable(body).height(Fill).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vaults_are_read_from_show_ledger() {
        let show = vec![
            "chain: 3 entries, head ab, at /x".to_string(),
            "payments: ledger id 01, retention 1 s".to_string(),
            "app_secrets: vault id 02, retention 7776000 s".to_string(),
        ];
        assert_eq!(LedgerPage::vault_names(&show), vec!["app_secrets".to_string()]);
    }
}
