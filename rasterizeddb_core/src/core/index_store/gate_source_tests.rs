//! The write gate's source test (hook C10; docs/persistent-indexes.md §2 and
//! §13 V3).
//!
//! The proofs in the mutators' signatures fence the mutators in only while
//! nothing but the mints in `gate.rs` can build a token. A visibility rule
//! cannot say that: private fields are visible to `gate`'s descendants, and a
//! token can be handed out without being returned. So this parses every file
//! of the crate with `syn` (it neither expands macros nor resolves names, so
//! macro bodies are read as tokens and names are pinned) and checks:
//!
//! 1. **Construction and reach.** A `WriteTicket` or `BuildToken` literal (or
//!    `Self { .. }` in an `impl` of one) appears only in the body of its mint.
//!    Nothing else names either token in a return type, a closure's return
//!    type, an `Fn*` bound, an associated type, a `static`, a `const`, a field
//!    or a `macro_rules!` body; no `type` alias or `use … as` renames a token
//!    or `Proof`; neither token implements `Clone`, `Copy`, `Default`, `From`
//!    or anything else but `Drop`, nor derives anything but `Debug`.
//!    `Proof` is gate.rs's struct of a private `ProofKind` and a borrow, built
//!    (a literal, or `Self` returned from its impls) only by its two
//!    `From<&token>` impls, its fields touched only there and in `kind`;
//!    neither it nor `ProofKind` implements or derives `Default` or any other
//!    trait than those, and no other impl names either in its trait. The
//!    `BuildToken` mint is called only from the named rebuild paths, and
//!    `test_mint` only from `cfg(test)` or `test_tokens` code. No `unsafe`
//!    anywhere in `index_store`, and none, nor `forget`, `ManuallyDrop` or
//!    `leak`, in any fn that names a token or `Proof`: every way to duplicate
//!    or leak a ticket needs one of them. Macro invocations are scanned as
//!    tokens and, where they parse as statements or expressions, visited like
//!    any other code. A `#[path]` or `include!` that leaves `src/` fails.
//! 2. **The proof is in every mutator.** Every fn of every `impl` block of
//!    `LiveIndex`, `GinIndex`, `TableIndexes` and `TableFilters`, at any
//!    visibility and inherent or trait, takes a `Proof`, unless it is on one
//!    of the named lists below; a private helper on that list is called only
//!    from fns that take a `Proof`. A new method fails here until someone
//!    puts it on a list or gives it a proof.
//!
//! Not checked here: that the right structure's gate was ticketed (the
//! census, V3), and that the types' private fields are touched only inside
//! their own impls (§2 part 3), which needs field types that `syn` does not
//! have; `ColumnFilter`'s mutators are reached only through
//! `TableFilters.columns`, a private field, so through `TableFilters`'s
//! proof-taking methods.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};

/// The tokens.
const TOKENS: [&str; 2] = ["WriteTicket", "BuildToken"];
/// The names no alias, rename, return type, bound or field may carry.
const NAMES: [&str; 3] = ["WriteTicket", "BuildToken", "Proof"];
/// `Proof`'s fields, the only ones a forger could set.
const PROOF_FIELDS: [&str; 2] = ["kind", "_borrow"];
/// The traits `Proof` implements by hand: `From` a borrowed token, nothing else.
const PROOF_FROM: [&str; 2] = ["From < & 'a WriteTicket >", "From < & 'a BuildToken >"];
/// What `Proof` and `ProofKind` may derive. Never `Default`.
const PROOF_DERIVES: [&str; 3] = ["Debug", "Clone", "Copy"];
const PROOF_KIND_DERIVES: [&str; 5] = ["Debug", "Clone", "Copy", "PartialEq", "Eq"];
/// Where the tokens live, relative to `src/`.
const GATE: &str = "core/index_store/gate.rs";
/// The mints, the only fns that may build a token: `(fn, the token it builds)`.
const MINTS: [(&str, &str); 3] = [
    ("ticket", "WriteTicket"),
    ("build_token", "BuildToken"),
    ("test_mint", "WriteTicket"),
];
/// The rebuild paths that may mint a `BuildToken`: `(file, fn)`. Each builds
/// or clears a structure that is not ready (§2, `BuildToken`).
const BUILD_TOKEN_CALLERS: [(&str, &str); 4] = [
    // The open path, shared by a plain table and a partition: the unindexed
    // prefix on a filter not yet installed, and the open-time index build's
    // readiness.
    ("core/row/table.rs", "assemble"),
    // VACUUM FULL's rebuild of the indexes it made unready.
    ("core/row/table.rs", "compact"),
    // The filter backfill.
    ("core/row/table.rs", "backfill_into"),
    // The index build.
    ("core/row/table.rs", "build_indexes"),
];
/// Macros that may name `Proof` (never a token).
const MACROS_NAMING_PROOF: [&str; 1] = ["matches"];
/// Fields that may store an already-minted token: `(struct, field)`. The
/// family's lock-owner record (§2, Ticket ownership; slice B11), and
/// `VACUUM FULL`'s renumbering guard, which borrows the compaction's ticket
/// for the mutation its drop makes.
const FIELDS_HOLDING_A_TOKEN: [(&str, &str); 2] = [("FamilyShard", "held"), ("RenumberingGuard", "ticket")];

/// The types whose every fn takes a `Proof` unless listed below.
const GUARDED: [&str; 4] = ["LiveIndex", "GinIndex", "TableIndexes", "TableFilters"];

/// Fns that build a new structure, which nothing else can reach yet.
const CONSTRUCTORS: [(&str, &str); 8] = [
    ("LiveIndex", "new"),
    ("LiveIndex", "over_path"),
    ("GinIndex", "new"),
    ("TableIndexes", "new"),
    ("TableIndexes", "with_gin"),
    ("TableFilters", "new"),
    ("TableFilters", "for_table"),
    ("TableFilters", "from_checkpoint"),
];

/// Fns that change nothing: lookups, planning, sizes, keys, readiness and
/// epochs read, and the read-only view.
const READ_ONLY: [(&str, &str); 66] = [
    ("LiveIndex", "method"),
    ("LiveIndex", "leading"),
    ("LiveIndex", "key_of"),
    ("LiveIndex", "key_of_values"),
    ("LiveIndex", "key_of_row"),
    ("LiveIndex", "shard"),
    ("LiveIndex", "holds"),
    ("LiveIndex", "sizes"),
    ("LiveIndex", "lookup_eq"),
    ("LiveIndex", "lookup_range"),
    ("LiveIndex", "slots_for_key"),
    ("LiveIndex", "append_unkeyed"),
    ("LiveIndex", "listed_under"),
    ("LiveIndex", "retain_under"),
    // The sorted stream a snapshot captures (hook C3, B16).
    ("LiveIndex", "for_each_sorted"),
    ("GinIndex", "shape"),
    ("GinIndex", "shard"),
    ("GinIndex", "holds"),
    ("GinIndex", "walked_lists"),
    ("GinIndex", "lookup"),
    ("GinIndex", "lookup_dense"),
    ("GinIndex", "estimate"),
    ("GinIndex", "estimate_and_cost"),
    ("GinIndex", "sizes"),
    ("GinIndex", "answers"),
    ("GinIndex", "query_for"),
    ("GinIndex", "for_each_sorted"),
    ("TableIndexes", "is_empty"),
    ("TableIndexes", "len"),
    ("TableIndexes", "view"),
    ("TableIndexes", "shape_hash"),
    ("TableIndexes", "path_column"),
    ("TableIndexes", "is_ready"),
    ("TableIndexes", "epoch"),
    ("TableIndexes", "exact"),
    ("TableIndexes", "covers"),
    ("TableIndexes", "columns"),
    ("TableIndexes", "keys_of"),
    ("TableIndexes", "holds"),
    ("TableIndexes", "plan"),
    ("TableIndexes", "plan_json"),
    ("TableIndexes", "ask_order"),
    ("TableIndexes", "plan_json_dense"),
    ("TableIndexes", "type_of"),
    ("TableIndexes", "eval"),
    ("TableIndexes", "eval_conjunction"),
    ("TableFilters", "resume_slot"),
    ("TableFilters", "sidecar_len"),
    ("TableFilters", "deltas_since_base"),
    ("TableFilters", "checkpoint_requested"),
    ("TableFilters", "should_checkpoint"),
    ("TableFilters", "config"),
    ("TableFilters", "is_empty"),
    ("TableFilters", "index_of"),
    ("TableFilters", "column_is_monotonic"),
    ("TableFilters", "indexes"),
    ("TableFilters", "renumber_epoch"),
    ("TableFilters", "is_complete"),
    ("TableFilters", "bytes"),
    ("TableFilters", "stats"),
    ("TableFilters", "evaluate"),
    // Whether a clear would change a plane: vacuum asks before it takes a
    // ticket (RV2).
    ("TableFilters", "any_codes_in_runs"),
    // A seal's FILTER section and its identity's configuration hash (B13).
    ("TableFilters", "sealed_image"),
    ("TableFilters", "config_hash"),
    // A new filter built from a snapshot's FILTER section (B14): reads
    // `self`, the rebuild's template, and mutates nothing of it.
    ("TableFilters", "load_sealed"),
    // The audit's per-slot question, the probe's bit for one slot (B18).
    ("TableFilters", "admits_eq"),
];

/// More read-only fns, kept apart only to keep the list above readable:
/// the filter's evaluation internals and a test's reference evaluator.
const READ_ONLY_EVAL: [(&str, &str); 3] = [
    ("TableFilters", "eval"),
    ("TableFilters", "eval_in_list"),
    ("TableFilters", "eval_general_or"),
];

/// The filter sidecar's own bookkeeping: the runtime checkpoint, its dirty
/// byte and its request flag. They change no entry and are not among §2's
/// mutators; phase 1 retires the runtime checkpoint (§5), and the sidecar
/// writes are `GatedIo`'s to gate, not C10's.
const SIDECAR: [(&str, &str); 7] = [
    ("TableFilters", "build"),
    ("TableFilters", "meta_of"),
    ("TableFilters", "checkpoint_bytes"),
    ("TableFilters", "plan"),
    ("TableFilters", "checkpoint"),
    ("TableFilters", "try_checkpoint"),
    ("TableFilters", "mark_disk_dirty"),
];

/// The request flag the maintenance task reads.
const SIDECAR_FLAGS: [(&str, &str); 1] = [("TableFilters", "request_checkpoint")];

/// Private helpers that mutate, called only from fns that take a `Proof`.
const HELPERS: [(&str, &str); 3] = [
    ("LiveIndex", "set_present"),
    ("GinIndex", "set_present"),
    ("TableFilters", "note_writes"),
];

fn src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// One parsed file and whether its module is test-only.
struct Source {
    rel: String,
    file: syn::File,
    test: bool,
}

/// Whether these attributes make an item test-only: `#[cfg(test)]`,
/// `#[cfg(feature = "test_tokens")]` (alone or in `any`/`all`), `#[test]`
/// or `#[tokio::test]`. A `not` anywhere in the `cfg` reads as not test-only.
fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        let path = a.path();
        if path.is_ident("test") || path.segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        if !path.is_ident("cfg") {
            return false;
        }
        let flat = flatten(a.meta.to_token_stream());
        let has = |n: &str| flat.iter().any(|t| matches!(t, Tok::Ident(i) if i == n));
        let lit = flat
            .iter()
            .any(|t| matches!(t, Tok::Lit(l) if l == "\"test_tokens\""));
        !has("not") && (has("test") || lit)
    })
}

fn path_attr(attrs: &[syn::Attribute]) -> Option<String> {
    attrs
        .iter()
        .find(|a| a.path().is_ident("path"))
        .and_then(|a| match &a.meta {
            syn::Meta::NameValue(nv) => match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) => Some(s.value()),
                _ => None,
            },
            _ => None,
        })
}

fn rel_of(path: &Path) -> String {
    path.strip_prefix(src_dir())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn inside_src(path: &Path) -> bool {
    match (path.canonicalize(), src_dir().canonicalize()) {
        (Ok(p), Ok(src)) => p.starts_with(src),
        _ => false,
    }
}

/// Follows `mod` declarations from the crate roots, carrying `cfg(test)`.
fn load(
    path: &Path,
    children: &Path,
    test: bool,
    out: &mut Vec<Source>,
    problems: &mut Vec<String>,
) {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let file = syn::parse_file(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let file_dir = path.parent().unwrap().to_path_buf();
    walk_mods(
        &file.items,
        &file_dir,
        children,
        0,
        test,
        out,
        problems,
        path,
    );
    out.push(Source {
        rel: rel_of(path),
        file,
        test,
    });
}

#[allow(clippy::too_many_arguments)]
fn walk_mods(
    items: &[syn::Item],
    file_dir: &Path,
    dir: &Path,
    depth: usize,
    test: bool,
    out: &mut Vec<Source>,
    problems: &mut Vec<String>,
    from: &Path,
) {
    for item in items {
        let syn::Item::Mod(m) = item else { continue };
        let test = test || is_test_only(&m.attrs);
        let name = m.ident.to_string();
        if let Some((_, inner)) = &m.content {
            walk_mods(
                inner,
                file_dir,
                &dir.join(&name),
                depth + 1,
                test,
                out,
                problems,
                from,
            );
            continue;
        }
        let target = match path_attr(&m.attrs) {
            Some(p) => (if depth == 0 {
                file_dir.to_path_buf()
            } else {
                dir.to_path_buf()
            })
            .join(p),
            None => {
                let flat = dir.join(format!("{name}.rs"));
                if flat.exists() {
                    flat
                } else {
                    dir.join(&name).join("mod.rs")
                }
            }
        };
        if !target.exists() {
            problems.push(format!(
                "{}: `mod {name}` names {}, which does not exist",
                rel_of(from),
                target.display()
            ));
            continue;
        }
        if !inside_src(&target) {
            problems.push(format!(
                "{}: `mod {name}` reaches {} outside src/",
                rel_of(from),
                target.display()
            ));
            continue;
        }
        let children = match target.file_name().and_then(|f| f.to_str()) {
            Some("mod.rs") => target.parent().unwrap().to_path_buf(),
            _ => target.with_extension(""),
        };
        load(&target, &children, test, out, problems);
    }
}

/// Every file of the crate: both roots' module trees, then any other `.rs`
/// file under `src/` (as not test-only), so nothing compiled in by
/// `include!` or a stray `#[path]` is skipped.
fn crate_sources(problems: &mut Vec<String>) -> Vec<Source> {
    let src = src_dir();
    let mut out = Vec::new();
    load(&src.join("lib.rs"), &src, false, &mut out, problems);
    load(&src.join("main.rs"), &src, false, &mut out, problems);
    let seen: BTreeSet<String> = out.iter().map(|s| s.rel.clone()).collect();
    let mut stack = vec![src.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") && !seen.contains(&rel_of(&p)) {
                let text = std::fs::read_to_string(&p).unwrap();
                match syn::parse_file(&text) {
                    Ok(file) => out.push(Source {
                        rel: rel_of(&p),
                        file,
                        test: false,
                    }),
                    Err(e) => problems.push(format!("{}: does not parse: {e}", rel_of(&p))),
                }
            }
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Punct(char),
    Lit(String),
    /// A group's opening delimiter: `(`, `[`, `{`, or ' ' for none.
    Open(char),
}

fn flatten(ts: TokenStream) -> Vec<Tok> {
    let mut out = Vec::new();
    fn go(ts: TokenStream, out: &mut Vec<Tok>) {
        for t in ts {
            match t {
                TokenTree::Ident(i) => out.push(Tok::Ident(i.to_string())),
                TokenTree::Punct(p) => out.push(Tok::Punct(p.as_char())),
                TokenTree::Literal(l) => out.push(Tok::Lit(l.to_string())),
                TokenTree::Group(g) => {
                    out.push(Tok::Open(match g.delimiter() {
                        proc_macro2::Delimiter::Parenthesis => '(',
                        proc_macro2::Delimiter::Bracket => '[',
                        proc_macro2::Delimiter::Brace => '{',
                        proc_macro2::Delimiter::None => ' ',
                    }));
                    go(g.stream(), out)
                }
            }
        }
    }
    go(ts, &mut out);
    out
}

fn idents(t: &impl ToTokens) -> BTreeSet<String> {
    flatten(t.to_token_stream())
        .into_iter()
        .filter_map(|t| match t {
            Tok::Ident(i) => Some(i),
            _ => None,
        })
        .collect()
}

fn mentions(t: &impl ToTokens, names: &[&str]) -> bool {
    let ids = idents(t);
    names.iter().any(|n| ids.contains(*n))
}

fn short(t: &impl ToTokens) -> String {
    let s = t.to_token_stream().to_string();
    if s.len() > 120 {
        format!(
            "{}…",
            &s[..s.char_indices().nth(120).map_or(s.len(), |(i, _)| i)]
        )
    } else {
        s
    }
}

/// The idents that duplicate or leak a ticket, or need `unsafe` to:
/// `unsafe` itself, `ManuallyDrop`, `leak` as a method or a path
/// (`Box::leak`, `v.leak()`), and `forget` as a path (`mem::forget`) or a
/// called free fn (`forget(t)` after an import). A method call `.forget(` is
/// the indexes' own mutator, and a variable or field may be named `forget`.
fn duplicating(flat: &[Tok]) -> Option<String> {
    for (i, t) in flat.iter().enumerate() {
        let Tok::Ident(id) = t else { continue };
        let prev = i.checked_sub(1).map(|p| &flat[p]);
        let next = flat.get(i + 1);
        let after_dot = matches!(prev, Some(Tok::Punct('.')));
        // `::`, not a field's or a binding's `:`.
        let after_path = i >= 2 && flat[i - 1] == Tok::Punct(':') && flat[i - 2] == Tok::Punct(':');
        let after_fn = prev == Some(&Tok::Ident("fn".into()));
        match id.as_str() {
            "unsafe" | "ManuallyDrop" => return Some(id.clone()),
            "leak" if after_dot || after_path => return Some(id.clone()),
            "forget"
                if after_path || (next == Some(&Tok::Open('(')) && !after_dot && !after_fn) =>
            {
                return Some(id.clone());
            }
            _ => {}
        }
    }
    None
}

#[derive(Clone)]
struct FnCtx {
    name: String,
    takes_proof: bool,
    /// Names a token or `Proof` anywhere, macro bodies included.
    names_gate: bool,
}

/// What the scan records for part 2.
#[derive(Default)]
struct Census {
    /// `(type, fn, takes a proof)` for every fn of a guarded type's impls.
    fns: Vec<(String, String, bool, String)>,
    /// `(helper, file, caller fn, caller takes a proof or is test code)`.
    helper_calls: Vec<(String, String, String, bool)>,
}

struct Scan<'a> {
    rel: &'a str,
    problems: &'a mut BTreeSet<String>,
    census: &'a mut Census,
    test: Vec<bool>,
    fns: Vec<FnCtx>,
    impls: Vec<String>,
    /// Each entry of `impls`'s trait, `""` for an inherent impl.
    impl_traits: Vec<String>,
    mods: usize,
    token_defs: &'a mut BTreeMap<String, usize>,
}

impl Scan<'_> {
    fn fail(&mut self, what: String) {
        let at = self
            .fns
            .last()
            .map_or(String::new(), |f| format!(" in fn {}", f.name));
        self.problems.insert(format!("{}{at}: {what}", self.rel));
    }

    fn in_test(&self) -> bool {
        *self.test.last().unwrap()
    }

    fn in_index_store(&self) -> bool {
        self.rel.starts_with("core/index_store/")
    }

    /// The innermost named fn; closures and blocks are its body.
    fn current_fn(&self) -> Option<&FnCtx> {
        self.fns.last()
    }

    /// Whether the innermost fn is `from` of one of `Proof`'s two `From`
    /// impls at the top of `gate.rs`: the only place a `Proof` is built.
    fn in_proof_from(&self) -> bool {
        self.rel == GATE
            && self.mods == 0
            && self.impls.len() == 1
            && self.impls[0] == "Proof"
            && PROOF_FROM.contains(&self.impl_traits[0].as_str())
            && self.fns.len() == 1
            && self.fns[0].name == "from"
    }

    /// The derives and other attributes of `Proof` or `ProofKind`.
    fn check_proof_attrs(&mut self, name: &str, attrs: &[syn::Attribute], allowed: &[&str]) {
        for a in attrs {
            let ids = idents(&a.meta);
            if ids.contains("Default") {
                self.fail(format!("{name} has a Default: {}", short(&a.meta)));
            }
            if a.path().is_ident("derive")
                && let Some(d) = ids.iter().find(|d| *d != "derive" && !allowed.contains(&d.as_str()))
            {
                self.fail(format!("{name} derives {d}"));
            }
        }
    }

    /// Whether the innermost fn is a free fn at the top of `gate.rs` named `name`.
    fn in_mint(&self, name: &str) -> bool {
        self.rel == GATE
            && self.mods == 0
            && self.impls.is_empty()
            && self.fns.len() == 1
            && self.fns[0].name == name
    }

    fn enter_fn(
        &mut self,
        attrs: &[syn::Attribute],
        sig: &syn::Signature,
        whole: &impl ToTokens,
        body: impl FnOnce(&mut Self),
    ) {
        let name = sig.ident.to_string();
        let takes_proof = sig.inputs.iter().any(|a| match a {
            syn::FnArg::Typed(pt) => mentions(&*pt.ty, &["Proof"]),
            syn::FnArg::Receiver(_) => false,
        });
        let flat = flatten(whole.to_token_stream());
        let names_gate = flat
            .iter()
            .any(|t| matches!(t, Tok::Ident(i) if NAMES.contains(&i.as_str())));
        self.test.push(self.in_test() || is_test_only(attrs));
        // A mint is a free fn at the top of gate.rs, so the check runs before
        // this fn is pushed.
        let is_mint =
            self.rel == GATE && self.mods == 0 && self.impls.is_empty() && self.fns.is_empty();
        if let syn::ReturnType::Type(_, ty) = &sig.output {
            let mint_ok = is_mint
                && MINTS
                    .iter()
                    .any(|(m, t)| *m == name && mentions(&**ty, &[t]));
            if mentions(&**ty, &TOKENS) && !mint_ok {
                self.fail(format!("fn {name} returns a token: {}", short(&**ty)));
            }
            if mentions(&**ty, &["Proof"]) {
                self.fail(format!("fn {name} returns a Proof: {}", short(&**ty)));
            }
            // `fn from(_: &'a WriteTicket) -> Self` is checked once this fn is
            // pushed; any other fn returning `Self` from `Proof`'s impls forges.
            let from_ok = self.rel == GATE
                && self.mods == 0
                && self.fns.is_empty()
                && name == "from"
                && self.impl_traits.len() == 1
                && PROOF_FROM.contains(&self.impl_traits[0].as_str());
            if self.impls.last().is_some_and(|t| t == "Proof" || t == "ProofKind")
                && mentions(&**ty, &["Self"])
                && !from_ok
            {
                self.fail(format!("fn {name} returns Self from {}'s impl", self.impls.last().unwrap()));
            }
        }
        if (names_gate || self.in_index_store())
            && let Some(bad) = duplicating(&flat)
        {
            self.fail(format!("fn {name} names the gate and uses `{bad}`"));
        }
        if let Some(ty) = self.impls.last().cloned()
            && GUARDED.contains(&ty.as_str())
            && self.fns.is_empty()
        {
            self.census
                .fns
                .push((ty, name.clone(), takes_proof, self.rel.to_string()));
        }
        self.fns.push(FnCtx {
            name,
            takes_proof,
            names_gate,
        });
        body(self);
        self.fns.pop();
        self.test.pop();
    }

    fn check_mint_path(&mut self, path: &syn::Path) {
        let Some(last) = path.segments.last() else {
            return;
        };
        match last.ident.to_string().as_str() {
            "build_token" => {
                let caller = self.fns.first().map(|f| f.name.clone()).unwrap_or_default();
                let named = BUILD_TOKEN_CALLERS
                    .iter()
                    .any(|(f, n)| *f == self.rel && *n == caller);
                if !named {
                    self.fail("mints a BuildToken outside the named rebuild paths".into());
                }
            }
            "test_mint" if !self.in_test() => {
                self.fail("calls test_mint outside cfg(test)/test_tokens".into())
            }
            _ => {}
        }
    }

    fn check_macro(&mut self, mac: &syn::Macro) {
        let name = mac
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        let flat = flatten(mac.tokens.clone());
        let ids: BTreeSet<&str> = flat
            .iter()
            .filter_map(|t| match t {
                Tok::Ident(i) => Some(i.as_str()),
                _ => None,
            })
            .collect();
        if name == "include" {
            match flat.first() {
                Some(Tok::Lit(l)) => {
                    let p = Path::new(&src_dir())
                        .join(self.rel)
                        .parent()
                        .unwrap()
                        .join(l.trim_matches('"'));
                    if !inside_src(&p) {
                        self.fail(format!("include! reaches {} outside src/", p.display()));
                    }
                }
                _ => self.fail("include! of a path this test cannot read".into()),
            }
        }
        if TOKENS.iter().any(|t| ids.contains(t)) {
            self.fail(format!("macro {name}! names a token: {}", short(mac)));
        } else if ids.contains("Proof") && !MACROS_NAMING_PROOF.contains(&name.as_str()) {
            self.fail(format!("macro {name}! names Proof: {}", short(mac)));
        }
        if self
            .impls
            .last()
            .is_some_and(|t| TOKENS.contains(&t.as_str()))
            && ids.contains("Self")
        {
            self.fail(format!("macro {name}! names Self in a token's impl"));
        }
        if ids.contains("test_mint") && !self.in_test() {
            self.fail(format!(
                "macro {name}! names test_mint outside cfg(test)/test_tokens"
            ));
        }
        if ids.contains("build_token") {
            self.fail(format!("macro {name}! names the BuildToken mint"));
        }
        if (self.current_fn().is_some_and(|f| f.names_gate) || self.in_index_store())
            && let Some(bad) = duplicating(&flat)
        {
            self.fail(format!(
                "macro {name}! in a fn that names the gate uses `{bad}`"
            ));
        }
        // Then read the body as code where it is code.
        if let Ok(stmts) = syn::Block::parse_within.parse2(mac.tokens.clone()) {
            for s in &stmts {
                self.visit_stmt(s);
            }
        } else if let Ok(exprs) =
            Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated.parse2(mac.tokens.clone())
        {
            for e in &exprs {
                self.visit_expr(e);
            }
        }
    }

    fn check_fields(&mut self, owner: &str, fields: &syn::Fields) {
        for (i, f) in fields.iter().enumerate() {
            let fname = f.ident.as_ref().map_or(i.to_string(), |n| n.to_string());
            let allowed = FIELDS_HOLDING_A_TOKEN
                .iter()
                .any(|(s, n)| *s == owner && *n == fname);
            if mentions(&f.ty, &NAMES) && !allowed {
                self.fail(format!(
                    "field {owner}.{fname} holds a token or a Proof: {}",
                    short(&f.ty)
                ));
            }
        }
    }
}

impl<'ast> Visit<'ast> for Scan<'_> {
    fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
        self.test.push(self.in_test() || is_test_only(&m.attrs));
        self.mods += 1;
        visit::visit_item_mod(self, m);
        self.mods -= 1;
        self.test.pop();
    }

    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        self.enter_fn(&f.attrs, &f.sig, f, |s| visit::visit_item_fn(s, f));
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        self.enter_fn(&f.attrs, &f.sig, f, |s| visit::visit_impl_item_fn(s, f));
    }

    fn visit_trait_item_fn(&mut self, f: &'ast syn::TraitItemFn) {
        self.enter_fn(&f.attrs, &f.sig, f, |s| visit::visit_trait_item_fn(s, f));
    }

    fn visit_foreign_item_fn(&mut self, f: &'ast syn::ForeignItemFn) {
        if mentions(&f.sig, &NAMES) {
            self.fail(format!("foreign fn {} names the gate", f.sig.ident));
        }
        visit::visit_foreign_item_fn(self, f);
    }

    fn visit_item_impl(&mut self, i: &'ast syn::ItemImpl) {
        let self_ty = match &*i.self_ty {
            syn::Type::Path(p) => p
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_default(),
            other => short(other),
        };
        if i.unsafety.is_some() && (self.in_index_store() || mentions(&*i.self_ty, &NAMES)) {
            self.fail(format!("unsafe impl for {self_ty}"));
        }
        if TOKENS.contains(&self_ty.as_str())
            && let Some((_, tr, _)) = &i.trait_
        {
            let tr_name = tr
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_default();
            if tr_name != "Drop" {
                self.fail(format!(
                    "impl {tr_name} for {self_ty}: a token implements only Drop"
                ));
            }
        }
        // `impl<T> Trait for Wrapper<T>` with a token as `T` elsewhere, or a
        // `Deref<Target = WriteTicket>` guard: no associated type names one.
        for item in &i.items {
            if let syn::ImplItem::Type(t) = item
                && mentions(&t.ty, &NAMES)
            {
                self.fail(format!(
                    "impl for {self_ty}: associated type {} names the gate",
                    t.ident
                ));
            }
        }
        let tr = i
            .trait_
            .as_ref()
            .and_then(|(_, tr, _)| tr.segments.last())
            .map(short)
            .unwrap_or_default();
        let proof_from = self.rel == GATE
            && self.mods == 0
            && self_ty == "Proof"
            && PROOF_FROM.contains(&tr.as_str());
        if (self_ty == "Proof" || self_ty == "ProofKind") && !tr.is_empty() && !proof_from {
            self.fail(format!("impl {tr} for {self_ty}: Proof implements only From<&token>"));
        }
        if let Some((_, path, _)) = &i.trait_
            && mentions(path, &["Proof", "ProofKind"])
            && !proof_from
        {
            self.fail(format!("impl {tr} for {self_ty} names Proof in its trait"));
        }
        let test = is_test_only(&i.attrs);
        self.test.push(self.in_test() || test);
        self.impls.push(self_ty);
        self.impl_traits.push(tr);
        visit::visit_item_impl(self, i);
        self.impl_traits.pop();
        self.impls.pop();
        self.test.pop();
    }

    fn visit_item_struct(&mut self, s: &'ast syn::ItemStruct) {
        let name = s.ident.to_string();
        if TOKENS.contains(&name.as_str()) {
            *self.token_defs.entry(name.clone()).or_default() += 1;
            if self.rel != GATE || self.mods != 0 {
                self.fail(format!("struct {name} defined outside the top of gate.rs"));
            }
            match &s.fields {
                syn::Fields::Named(n) => {
                    if n.named
                        .iter()
                        .any(|f| !matches!(f.vis, syn::Visibility::Inherited))
                    {
                        self.fail(format!("struct {name} has a field that is not private"));
                    }
                }
                _ => self.fail(format!(
                    "struct {name} is not a braced struct with named fields"
                )),
            }
            for a in &s.attrs {
                if a.path().is_ident("derive") {
                    let derived = idents(&a.meta);
                    if derived.iter().any(|d| d != "derive" && d != "Debug") {
                        self.fail(format!(
                            "struct {name} derives more than Debug: {}",
                            short(&a.meta)
                        ));
                    }
                }
            }
        } else if name == "Proof" {
            *self.token_defs.entry(name.clone()).or_default() += 1;
            let shape: Vec<String> = s
                .fields
                .iter()
                .map(|f| {
                    let vis = if matches!(f.vis, syn::Visibility::Inherited) { "" } else { "pub " };
                    format!("{vis}{}: {}", f.ident.as_ref().map_or(String::new(), |i| i.to_string()), short(&f.ty))
                })
                .collect();
            let want = ["kind: ProofKind", "_borrow: PhantomData < & 'a () >"];
            if self.rel != GATE || self.mods != 0 || shape != want {
                self.fail(format!(
                    "struct Proof is not gate.rs's private kind and borrow: {shape:?}"
                ));
            }
            self.check_proof_attrs(&name, &s.attrs, &PROOF_DERIVES);
        } else {
            self.check_fields(&name, &s.fields);
        }
        visit::visit_item_struct(self, s);
    }

    fn visit_item_enum(&mut self, e: &'ast syn::ItemEnum) {
        let name = e.ident.to_string();
        if name == "Proof" {
            *self.token_defs.entry(name.clone()).or_default() += 1;
            self.fail("enum Proof: Proof is gate.rs's struct".into());
        } else if name == "ProofKind" {
            *self.token_defs.entry(name.clone()).or_default() += 1;
            let shape: Vec<String> = e
                .variants
                .iter()
                .map(|v| format!("{}{}", v.ident, short(&v.fields)))
                .collect();
            if self.rel != GATE || self.mods != 0 || shape != ["Ticket", "Build"] {
                self.fail(format!(
                    "enum ProofKind is not gate.rs's two unit variants: {shape:?}"
                ));
            }
            self.check_proof_attrs(&name, &e.attrs, &PROOF_KIND_DERIVES);
        } else {
            for v in &e.variants {
                self.check_fields(&format!("{name}::{}", v.ident), &v.fields);
            }
        }
        visit::visit_item_enum(self, e);
    }

    fn visit_item_union(&mut self, u: &'ast syn::ItemUnion) {
        let name = u.ident.to_string();
        for f in &u.fields.named {
            if mentions(&f.ty, &NAMES) {
                self.fail(format!("union {name} holds a token or a Proof"));
            }
        }
        visit::visit_item_union(self, u);
    }

    fn visit_item_type(&mut self, t: &'ast syn::ItemType) {
        if mentions(&*t.ty, &NAMES) || NAMES.contains(&t.ident.to_string().as_str()) {
            self.fail(format!("type alias {} names the gate", t.ident));
        }
        visit::visit_item_type(self, t);
    }

    fn visit_trait_item_type(&mut self, t: &'ast syn::TraitItemType) {
        if mentions(t, &NAMES) {
            self.fail(format!("associated type {} names the gate", t.ident));
        }
        visit::visit_trait_item_type(self, t);
    }

    fn visit_item_static(&mut self, s: &'ast syn::ItemStatic) {
        if mentions(&*s.ty, &NAMES) {
            self.fail(format!("static {} holds a token or a Proof", s.ident));
        }
        visit::visit_item_static(self, s);
    }

    fn visit_item_const(&mut self, c: &'ast syn::ItemConst) {
        if mentions(&*c.ty, &NAMES) {
            self.fail(format!("const {} holds a token or a Proof", c.ident));
        }
        visit::visit_item_const(self, c);
    }

    fn visit_impl_item_const(&mut self, c: &'ast syn::ImplItemConst) {
        if mentions(&c.ty, &NAMES) {
            self.fail(format!("const {} holds a token or a Proof", c.ident));
        }
        visit::visit_impl_item_const(self, c);
    }

    fn visit_trait_item_const(&mut self, c: &'ast syn::TraitItemConst) {
        if mentions(&c.ty, &NAMES) {
            self.fail(format!("const {} holds a token or a Proof", c.ident));
        }
        visit::visit_trait_item_const(self, c);
    }

    fn visit_use_rename(&mut self, r: &'ast syn::UseRename) {
        let from = r.ident.to_string();
        let to = r.rename.to_string();
        let mints: Vec<&str> = MINTS.iter().map(|(m, _)| *m).collect();
        if NAMES.contains(&from.as_str())
            || NAMES.contains(&to.as_str())
            || mints.contains(&from.as_str())
        {
            self.fail(format!("use … {from} as {to} renames the gate"));
        }
        visit::visit_use_rename(self, r);
    }

    fn visit_use_name(&mut self, n: &'ast syn::UseName) {
        match n.ident.to_string().as_str() {
            "build_token" => self.fail("imports the BuildToken mint; call it by its path".into()),
            "test_mint" if !self.in_test() => {
                self.fail("imports test_mint outside cfg(test)/test_tokens".into())
            }
            _ => {}
        }
        visit::visit_use_name(self, n);
    }

    fn visit_item_macro(&mut self, m: &'ast syn::ItemMacro) {
        if m.mac.path.is_ident("macro_rules") {
            let mints: Vec<&str> = MINTS.iter().map(|(m, _)| *m).collect();
            let ids = idents(&m.mac.tokens);
            if NAMES.iter().chain(mints.iter()).any(|n| ids.contains(*n)) {
                let name = m.ident.as_ref().map_or(String::new(), |i| i.to_string());
                self.fail(format!("macro_rules! {name} names the gate"));
            }
            return;
        }
        visit::visit_item_macro(self, m);
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        self.check_macro(m);
        visit::visit_macro(self, m);
    }

    fn visit_expr_struct(&mut self, e: &'ast syn::ExprStruct) {
        let last = e
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        let ty = if last == "Self" {
            self.impls.last().cloned().unwrap_or_default()
        } else {
            last.clone()
        };
        if ty == "Proof" && !self.in_proof_from() {
            self.fail("builds a Proof outside its From<&token> impls".into());
        }
        if TOKENS.contains(&ty.as_str()) {
            if last == "Self" {
                self.fail(format!("builds {ty} as Self {{ .. }} in its impl"));
            } else {
                let ok = MINTS.iter().any(|(m, t)| *t == ty && self.in_mint(m));
                if !ok {
                    self.fail(format!("builds {ty} outside its mint"));
                }
            }
        }
        visit::visit_expr_struct(self, e);
    }

    fn visit_expr_field(&mut self, e: &'ast syn::ExprField) {
        // Only `gate`'s module can reach `Proof`'s private fields at all.
        if let syn::Member::Named(m) = &e.member
            && self.rel.starts_with("core/index_store/gate")
            && !self.rel.contains("source_tests")
            && PROOF_FIELDS.contains(&m.to_string().as_str())
        {
            let getter = self.impls.last().is_some_and(|t| t == "Proof")
                && self.fns.len() == 1
                && self.fns[0].name == "kind";
            if !getter {
                self.fail(format!("touches Proof's field {m} outside Proof::kind"));
            }
        }
        visit::visit_expr_field(self, e);
    }

    fn visit_expr_path(&mut self, e: &'ast syn::ExprPath) {
        self.check_mint_path(&e.path);
        visit::visit_expr_path(self, e);
    }

    fn visit_expr_method_call(&mut self, e: &'ast syn::ExprMethodCall) {
        let name = e.method.to_string();
        if HELPERS.iter().any(|(_, h)| *h == name) {
            let (caller, proof) = self
                .fns
                .first()
                .map_or((String::new(), false), |f| (f.name.clone(), f.takes_proof));
            let ok = proof || self.in_test();
            self.census
                .helper_calls
                .push((name, self.rel.to_string(), caller, ok));
        }
        visit::visit_expr_method_call(self, e);
    }

    fn visit_expr_unsafe(&mut self, e: &'ast syn::ExprUnsafe) {
        if self.in_index_store() || self.current_fn().is_some_and(|f| f.names_gate) {
            self.fail("unsafe block".into());
        }
        visit::visit_expr_unsafe(self, e);
    }

    fn visit_expr_closure(&mut self, c: &'ast syn::ExprClosure) {
        if let syn::ReturnType::Type(_, ty) = &c.output
            && mentions(&**ty, &NAMES)
        {
            self.fail(format!("closure returns {}", short(&**ty)));
        }
        visit::visit_expr_closure(self, c);
    }

    fn visit_parenthesized_generic_arguments(
        &mut self,
        p: &'ast syn::ParenthesizedGenericArguments,
    ) {
        if mentions(p, &NAMES) {
            self.fail(format!("Fn* bound names the gate: {}", short(p)));
        }
        visit::visit_parenthesized_generic_arguments(self, p);
    }
}

/// Runs the scan over the whole crate and returns every problem.
fn scan_crate() -> (BTreeSet<String>, Census, BTreeMap<String, usize>) {
    let mut load_problems = Vec::new();
    let sources = crate_sources(&mut load_problems);
    let mut problems: BTreeSet<String> = load_problems.into_iter().collect();
    let mut census = Census::default();
    let mut token_defs = BTreeMap::new();
    for s in &sources {
        let mut scan = Scan {
            rel: &s.rel,
            problems: &mut problems,
            census: &mut census,
            test: vec![s.test],
            fns: Vec::new(),
            impls: Vec::new(),
            impl_traits: Vec::new(),
            mods: 0,
            token_defs: &mut token_defs,
        };
        scan.visit_file(&s.file);
    }
    (problems, census, token_defs)
}

#[test]
fn only_the_mints_build_a_token_and_nothing_else_reaches_one() {
    let (mut problems, _, defs) = scan_crate();
    for name in NAMES.iter().chain(&["ProofKind"]) {
        if defs.get(*name) != Some(&1) {
            problems.insert(format!(
                "{name} is defined {} times, not once",
                defs.get(*name).unwrap_or(&0)
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the write gate's reach:\n  {}",
        problems.into_iter().collect::<Vec<_>>().join("\n  ")
    );
}

#[test]
fn every_mutator_of_an_index_or_a_filter_takes_a_proof() {
    let (_, census, _) = scan_crate();
    let listed = |ty: &str, f: &str| {
        CONSTRUCTORS
            .iter()
            .chain(READ_ONLY.iter())
            .chain(READ_ONLY_EVAL.iter())
            .chain(SIDECAR.iter())
            .chain(SIDECAR_FLAGS.iter())
            .chain(HELPERS.iter())
            .any(|(t, n)| *t == ty && *n == f)
    };
    let mut problems = BTreeSet::new();
    for (ty, f, proof, file) in &census.fns {
        if !proof && !listed(ty, f) {
            problems.insert(format!(
                "{file}: {ty}::{f} takes no Proof and is on no list"
            ));
        }
        if *proof && listed(ty, f) {
            problems.insert(format!(
                "{file}: {ty}::{f} takes a Proof but is listed as needing none"
            ));
        }
    }
    // A list entry that names nothing is a stale exemption.
    let all = CONSTRUCTORS
        .iter()
        .chain(READ_ONLY.iter())
        .chain(READ_ONLY_EVAL.iter())
        .chain(SIDECAR.iter())
        .chain(SIDECAR_FLAGS.iter())
        .chain(HELPERS.iter());
    for (t, n) in all {
        if !census.fns.iter().any(|(ty, f, _, _)| ty == t && f == n) {
            problems.insert(format!("{t}::{n} is listed but no such fn exists"));
        }
    }
    for (helper, file, caller, proof) in &census.helper_calls {
        if !proof && !HELPERS.iter().any(|(_, h)| h == caller) {
            problems.insert(format!(
                "{file}: fn {caller} calls the mutating helper {helper} without a Proof"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "mutators without a proof:\n  {}",
        problems.into_iter().collect::<Vec<_>>().join("\n  ")
    );
}

/// `ColumnFilter`'s mutators take no proof; `TableFilters.columns` being
/// private is what keeps them behind `TableFilters`' proof-taking ones. A `pub`
/// there, or a method handing a column out mutably, would route around the
/// gate.
#[test]
fn the_column_filters_stay_behind_table_filters() {
    let path = src_dir().join("core/filter/mod.rs");
    let file: syn::File = syn::parse_file(&std::fs::read_to_string(&path).expect("read filter/mod.rs"))
        .expect("parse filter/mod.rs");
    let mut found = false;
    let mut problems = Vec::new();
    for item in &file.items {
        match item {
            syn::Item::Struct(s) if s.ident == "TableFilters" => {
                for field in &s.fields {
                    if field.ident.as_ref().is_some_and(|i| i == "columns") {
                        found = true;
                        if !matches!(field.vis, syn::Visibility::Inherited) {
                            problems.push("TableFilters.columns is not private".to_string());
                        }
                    }
                }
            }
            syn::Item::Impl(imp) if imp.self_ty.to_token_stream().to_string() == "TableFilters" => {
                for it in &imp.items {
                    if let syn::ImplItem::Fn(f) = it {
                        if matches!(f.vis, syn::Visibility::Inherited) {
                            continue;
                        }
                        let ret = f.sig.output.to_token_stream().to_string();
                        if ret.contains("ColumnFilter") && (ret.contains("mut") || ret.contains("RwLock") || ret.contains("Guard")) {
                            problems.push(format!("{} hands a ColumnFilter out: {}", f.sig.ident, ret));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    assert!(found, "TableFilters.columns not found; update this test");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
