//! The fence around the routing hash.
//!
//! `route_v1` is persisted: a row's point decides which partition's files it
//! is written to, and a query finds it again only by computing the same point.
//! Its one hash is the engine's in-tree XXH64, `core::hash::xxh64` (and its
//! streaming `Xxh64`), pinned to the C reference. `ahash`, `std`'s
//! `DefaultHasher` and every `RandomState` may change their output between
//! crate versions, CPU features, pointer widths and (seeded randomly)
//! processes, and rapidhash (`ItemFeed::rapid`, for GIN items alone) is frozen
//! to other values. Any of them reaching a point would compile, pass the tests
//! of the day it was written, and route every existing row somewhere its
//! queries never look. So would a second copy of XXH64 that drifted.
//!
//! So this parses every non-test item of every file in `core/partition/` and
//! refuses any path, method or macro identifier that names a hash (contains
//! `hash`, `xxh`, `crc`, `rapid` or `randomstate`, or is `ItemFeed`) other
//! than:
//!
//! - `xxh64` and `Xxh64` from `core::hash`: used through a path under
//!   `core::hash`, or by the bare name a `use` of that path brought in. No
//!   other member of `core::hash` (its `ItemFeed` builds rapidhash feeds),
//!   and no `xxh64` of any other origin: `core/partition/` may not define
//!   one either;
//! - `HashMap`, `HashSet`, `hash_map`, `hash_set`: collections, whose hasher
//!   orders a map and never reaches a value (a hasher named for one, such as
//!   `with_hasher` or a `RandomState` parameter, is still refused);
//! - names this directory defines itself (`level_hash`, a local `hashes`),
//!   whose bodies are checked like everything else;
//! - `crc32fast::hash` in `log.rs`, the map log's record checksum, which is
//!   framing and never routes.
//!
//! Attributes are not read, so `#[derive(Hash)]` passes. And positively:
//! `level_hash`, which every point is built from, must call `xxh64`, and
//! `key.rs` must import it from `core::hash`.
//!
//! `core/hash/source_tests.rs` fences rapidhash crate-wide (the identifier
//! `rapid` only in `jsonb.rs`'s `item_hasher`, `rapidhash` nowhere outside
//! `core/hash/`); this is the stricter fence for routing, and agrees with it.

use std::collections::BTreeSet;
use std::path::PathBuf;

use proc_macro2::{TokenStream, TokenTree};
use syn::visit::{self, Visit};

/// The routing hash: `core::hash`'s, by these names only.
const ROUTING_HASH: &[&str] = &["xxh64", "Xxh64"];

/// Collections, allowed anywhere here: their hasher never reaches a value.
const ALLOWED: &[&str] = &["HashMap", "HashSet", "hash_map", "hash_set"];

/// `(file, path)`: a hash allowed in one file only, for something that is
/// not routing.
const ALLOWED_IN: &[(&str, &str)] = &[("log.rs", "crc32fast::hash")];

fn names_a_hash(ident: &str) -> bool {
    let lower = ident.to_ascii_lowercase();
    ["hash", "xxh", "crc", "rapid", "randomstate", "itemfeed"]
        .iter()
        .any(|w| lower.contains(w))
}

/// Whether `item`'s attributes include `#[cfg(test)]`.
fn is_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg") && a.meta.require_list().is_ok_and(|l| l.tokens.to_string().trim() == "test")
    })
}

/// Every name this directory defines: fns, types, consts, statics, modules,
/// and bindings (locals, parameters, fields). A hash-like one is ours, so
/// using it by its bare name is not reaching a foreign hash.
#[derive(Default)]
struct Defined(BTreeSet<String>);

impl<'ast> Visit<'ast> for Defined {
    fn visit_signature(&mut self, s: &'ast syn::Signature) {
        self.0.insert(s.ident.to_string());
        visit::visit_signature(self, s);
    }
    fn visit_pat_ident(&mut self, p: &'ast syn::PatIdent) {
        self.0.insert(p.ident.to_string());
        visit::visit_pat_ident(self, p);
    }
    fn visit_item_struct(&mut self, i: &'ast syn::ItemStruct) {
        self.0.insert(i.ident.to_string());
        visit::visit_item_struct(self, i);
    }
    fn visit_item_enum(&mut self, i: &'ast syn::ItemEnum) {
        self.0.insert(i.ident.to_string());
        visit::visit_item_enum(self, i);
    }
    fn visit_item_const(&mut self, i: &'ast syn::ItemConst) {
        self.0.insert(i.ident.to_string());
        visit::visit_item_const(self, i);
    }
    fn visit_item_mod(&mut self, i: &'ast syn::ItemMod) {
        self.0.insert(i.ident.to_string());
        visit::visit_item_mod(self, i);
    }
    fn visit_item_static(&mut self, i: &'ast syn::ItemStatic) {
        self.0.insert(i.ident.to_string());
        visit::visit_item_static(self, i);
    }
    fn visit_field(&mut self, f: &'ast syn::Field) {
        if let Some(name) = &f.ident {
            self.0.insert(name.to_string());
        }
        visit::visit_field(self, f);
    }
}

/// The hash-naming identifiers `file` reaches outside its tests, as
/// `(where, path)`.
struct Reached<'a> {
    defined: &'a BTreeSet<String>,
    found: Vec<String>,
}

impl Reached<'_> {
    fn path(&mut self, segments: Vec<String>, what: &str) {
        let joined = segments.join("::");
        // Under `core::hash` (`crate::core::hash::…` or `core::hash::…`).
        let core_hash_at = segments.windows(2).position(|w| w[0] == "core" && w[1] == "hash");
        let under_core_hash = core_hash_at.is_some_and(|at| segments[..at].iter().all(|p| p == "crate"));
        let foreign = segments.iter().enumerate().any(|(i, s)| {
            names_a_hash(s)
                && !ALLOWED.contains(&s.as_str())
                // `core::hash` only on the way to its `xxh64` / `Xxh64`.
                && !(s == "hash"
                    && under_core_hash
                    && core_hash_at == i.checked_sub(1)
                    && segments.get(i + 1).is_some_and(|n| ROUTING_HASH.contains(&n.as_str())))
                // `xxh64` / `Xxh64` from `core::hash`, or by the bare name its
                // `use` brought in (a call, or `Xxh64::new`): never through
                // another module, which would be another copy.
                && !(ROUTING_HASH.contains(&s.as_str()) && (under_core_hash || i == 0))
                // Ours only when used bare or through `self`/`super`/`crate`
                // paths, never as a segment of someone else's path.
                && !(self.defined.contains(s)
                    && segments[..i].iter().all(|p| ["self", "super", "crate", "core", "partition"].contains(&p.as_str())))
        });
        if foreign {
            self.found.push(format!("{what} `{joined}`"));
        }
    }

    fn tokens(&mut self, tokens: TokenStream) {
        for t in tokens {
            match t {
                TokenTree::Ident(i) => {
                    let name = i.to_string();
                    let name = name.trim_start_matches("r#");
                    if names_a_hash(name)
                        && !ALLOWED.contains(&name)
                        && !ROUTING_HASH.contains(&name)
                        && !self.defined.contains(name)
                    {
                        self.found.push(format!("macro token `{name}`"));
                    }
                }
                TokenTree::Group(g) => self.tokens(g.stream()),
                _ => {}
            }
        }
    }
}

fn segments(path: &syn::Path) -> Vec<String> {
    path.segments.iter().map(|s| s.ident.to_string()).collect()
}

fn use_paths(tree: &syn::UseTree, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    match tree {
        syn::UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            use_paths(&p.tree, prefix, out);
            prefix.pop();
        }
        syn::UseTree::Name(n) => out.push([prefix.as_slice(), &[n.ident.to_string()]].concat()),
        syn::UseTree::Rename(r) => out.push([prefix.as_slice(), &[r.ident.to_string()]].concat()),
        syn::UseTree::Glob(_) => out.push([prefix.as_slice(), &["*".to_string()]].concat()),
        syn::UseTree::Group(g) => {
            for t in &g.items {
                use_paths(t, prefix, out);
            }
        }
    }
}

impl<'ast> Visit<'ast> for Reached<'_> {
    fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
        if !is_test(&m.attrs) {
            visit::visit_item_mod(self, m);
        }
    }
    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        if !is_test(&f.attrs) {
            visit::visit_item_fn(self, f);
        }
    }
    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        if !is_test(&f.attrs) {
            visit::visit_impl_item_fn(self, f);
        }
    }
    fn visit_item_use(&mut self, u: &'ast syn::ItemUse) {
        if is_test(&u.attrs) {
            return;
        }
        let mut out = Vec::new();
        use_paths(&u.tree, &mut Vec::new(), &mut out);
        for p in out {
            self.path(p, "use");
        }
    }
    fn visit_path(&mut self, p: &'ast syn::Path) {
        self.path(segments(p), "path");
        visit::visit_path(self, p);
    }
    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        self.path(vec![m.method.to_string()], "method");
        visit::visit_expr_method_call(self, m);
    }
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        self.path(segments(&m.path), "macro");
        self.tokens(m.tokens.clone());
    }
    // Attributes (`#[derive(Hash)]`, docs) are not code that runs.
    fn visit_attribute(&mut self, _: &'ast syn::Attribute) {}
}

fn partition_files() -> Vec<(String, syn::File)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/core/partition");
    let mut files: Vec<(String, syn::File)> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .map(|p| {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            let text = std::fs::read_to_string(&p).unwrap();
            let file = syn::parse_file(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
            (name, file)
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(
        ["key.rs", "map.rs", "route.rs", "table.rs", "guard.rs"]
            .iter()
            .all(|f| files.iter().any(|(n, _)| n == f)),
        "core/partition/ is not where this test expects it: {:?}",
        files.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    files
}

#[test]
fn routing_hashes_only_through_xxh64() {
    let files = partition_files();
    let mut defined = Defined::default();
    for (_, file) in &files {
        defined.visit_file(file);
    }
    // A second XXH64 here would be a copy that can drift from core::hash's.
    let copies: Vec<&String> = defined.0.iter().filter(|d| ROUTING_HASH.contains(&d.as_str())).collect();
    assert!(
        copies.is_empty(),
        "core/partition/ defines {copies:?}: routing hashes through core::hash::xxh64, never a copy"
    );
    let mut wrong = Vec::new();
    for (name, file) in &files {
        let mut reached = Reached {
            defined: &defined.0,
            found: Vec::new(),
        };
        reached.visit_file(file);
        let allowed_here: Vec<&str> = ALLOWED_IN.iter().filter(|(f, _)| f == name).map(|(_, p)| *p).collect();
        wrong.extend(
            reached
                .found
                .into_iter()
                .filter(|f| !allowed_here.iter().any(|p| f.ends_with(&format!("`{p}`"))))
                .map(|f| format!("{name}: {f}")),
        );
    }
    assert!(
        wrong.is_empty(),
        "core/partition/ reaches a hash other than xxh64; a routed point must never depend on one:\n  {}",
        wrong.join("\n  ")
    );
}

/// The positive half: the function every point is built from hashes with
/// `xxh64`, so a refactor that moved the hashing somewhere this test does not
/// look, or replaced it with arithmetic, is noticed here as well as by the
/// golden values in `key.rs`.
#[test]
fn level_hash_calls_xxh64() {
    use quote::ToTokens;
    let files = partition_files();
    let (_, key) = files.iter().find(|(n, _)| n == "key.rs").unwrap();
    let level_hash = key
        .items
        .iter()
        .find_map(|i| match i {
            syn::Item::Fn(f) if f.sig.ident == "level_hash" => Some(f),
            _ => None,
        })
        .expect("key.rs has no fn level_hash");
    fn calls(tokens: TokenStream, name: &str) -> usize {
        tokens
            .into_iter()
            .map(|t| match t {
                TokenTree::Ident(i) => (i == name) as usize,
                TokenTree::Group(g) => calls(g.stream(), name),
                _ => 0,
            })
            .sum()
    }
    assert_eq!(
        calls(level_hash.block.to_token_stream(), "xxh64"),
        1,
        "level_hash must hash with xxh64, once"
    );
    let imports_it = key.items.iter().any(|i| match i {
        syn::Item::Use(u) => {
            let mut out = Vec::new();
            use_paths(&u.tree, &mut Vec::new(), &mut out);
            out.iter().any(|p| p.join("::") == "crate::core::hash::xxh64")
        }
        _ => false,
    });
    assert!(imports_it, "key.rs must take xxh64 from crate::core::hash");
}

/// What [`Reached`] makes of `src`, a file's text, with nothing defined.
fn reached_in(src: &str) -> Vec<String> {
    let file = syn::parse_file(src).unwrap();
    let defined = BTreeSet::new();
    let mut reached = Reached {
        defined: &defined,
        found: Vec::new(),
    };
    reached.visit_file(&file);
    reached.found
}

/// The classifier on snippets: `core::hash`'s `xxh64` / `Xxh64` pass, and
/// everything else of it, rapidhash above all, is refused, however reached;
/// so is an `xxh64` from anywhere but `core::hash`.
#[test]
fn the_fence_admits_only_core_hash_xxh64() {
    for allowed in [
        "use crate::core::hash::xxh64; fn f(b: &[u8]) -> u64 { xxh64(b, 1) }",
        "use crate::core::hash::{Xxh64, xxh64}; fn f(b: &[u8]) -> u64 { crate::core::hash::xxh64::xxh64(b, 1) }",
        "fn f(b: &[u8]) -> u64 { let mut h = crate::core::hash::Xxh64::new(1); h.update(b); h.digest() }",
        "use std::collections::{HashMap, HashSet}; fn f() -> HashMap<u32, u32> { HashMap::new() }",
        "#[derive(Hash)] struct S; #[cfg(test)] mod tests { use ahash::RandomState; }",
    ] {
        assert_eq!(reached_in(allowed), Vec::<String>::new(), "refused: {allowed}");
    }
    for refused in [
        "use crate::core::hash::ItemFeed; fn f() { let _ = ItemFeed::rapid(1); }",
        "fn f() { let _ = crate::core::hash::ItemFeed::new(1); }",
        "use crate::core::hash::*;",
        "fn f(b: &[u8]) -> u64 { crate::core::hash::rapidhash::rapidhash(b) }",
        "fn f(b: &[u8]) -> u64 { rapidhash::rapidhash_seeded(b, 1) }",
        "fn f(b: &[u8]) -> u64 { std::hash::BuildHasher::hash_one(&ahash::RandomState::new(), b) }",
        "fn f() -> std::collections::HashMap<u32, u32, ahash::RandomState> { Default::default() }",
        "fn f() { let _ = std::collections::HashMap::<u32, u32>::with_hasher(Default::default()); }",
        "fn f(b: &[u8]) -> u64 { use std::hash::Hasher; let mut h = std::hash::DefaultHasher::new(); h.write(b); h.finish() }",
        "fn f(b: &[u8]) -> u32 { crc32fast::hash(b) }",
        "fn f(b: &[u8]) -> u64 { let x = 1; format!(\"{}\", fxhash::hash64(b)).len() as u64 + x }",
        "use super::xxh64::xxh64;",
        "fn f(b: &[u8]) -> u64 { super::xxh64::xxh64(b, 1) }",
        "fn f(b: &[u8]) -> u64 { twox_hash::xxh64::xxh64(b, 1) }",
        "fn f(b: &[u8]) -> u64 { xxhash_rust::xxh64::xxh64(b, 1) }",
        "fn f(b: &[u8]) -> u64 { other::core::hash::xxh64(b, 1) }",
    ] {
        assert!(!reached_in(refused).is_empty(), "passed: {refused}");
    }
}
