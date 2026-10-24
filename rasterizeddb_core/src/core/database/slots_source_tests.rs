//! The fence around the table maps (slice B3a; `docs/persistent-indexes.md`
//! §7, "a guard is an allowlist").
//!
//! `Database::table_slots` and `Database::partitioned_tables` are private, so
//! the compiler keeps every crate outside `core::database` off them (the
//! `compile_fail` doctest on [`super::slots`] shows it). Privacy does not
//! reach inside the module, so this reads every file under `src/` as tokens
//! (comments and doc text are not tokens) and checks:
//!
//! 1. The two map fields are named only in `slots.rs`, and in `mod.rs` by
//!    the struct and its constructor.
//! 2. The escape hatches `table_unlocked` and `each_table_unlocked`, which
//!    hand an executing statement a `Table` with no table lock, are called
//!    from exactly the functions listed in [`ESCAPES`], that many times
//!    each. B3b moves each caller to `table_for`; the list must then be
//!    empty but for the definitions.
//! 3. `table_for_tests` is named by no engine code (test files and
//!    `#[cfg(test)]` items may).
//! 4. Every lookup by name in `slots.rs` goes through `resolve`, the one
//!    place identifier case folding will drop in.
//!
//! All by name, so an unrelated local called `table_slots` fails too:
//! rename it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use syn::visit::{self, Visit};

const SLOTS: &str = "core/database/slots.rs";
const MOD: &str = "core/database/mod.rs";
const EXEC: &str = "core/rql/executor.rs";

/// Where the map fields may be named outside `slots.rs`: `(file, fn or
/// "<items>" for outside any fn, count)`.
const MAP_FIELDS: &[(&str, &str, &str, usize)] = &[
    ("table_slots", MOD, "<items>", 1),
    ("table_slots", MOD, "try_new_named", 4),
    ("partitioned_tables", MOD, "<items>", 1),
    ("partitioned_tables", MOD, "try_new_named", 1),
];

/// Every caller of the escape hatches, `(file, fn, count)`, with the two
/// definitions. B3b drains this to the definitions alone.
const ESCAPES: &[(&str, &str, usize)] = &[
    // The definitions.
    (SLOTS, "table_unlocked", 1),
    (SLOTS, "each_table_unlocked", 1),
    // DROP's internals (B4 runs them under AE).
    (MOD, "drop_table_to_depth", 1),
    // Walks over every table's files: FK sidecars, index declarations, the
    // rebalance plan.
    ("core/rql/column_declarations.rs", "other_foreign_keys", 1),
    ("core/rql/column_declarations.rs", "retarget_parent", 1),
    ("core/storage/relocate.rs", "plan", 1),
    // The executor (B3b: `table_for`, with the statement's owner and mode).
    (EXEC, "apply_conflict_update", 3),
    (EXEC, "check_conflict_target_infers_a_key", 1),
    (EXEC, "check_foreign_keys", 1),
    (EXEC, "default_literal_for", 1),
    (EXEC, "delete_using_join", 2),
    (EXEC, "ensure_shadow_column", 1),
    (EXEC, "execute_alter_table", 1),
    (EXEC, "execute_alter_table_action", 2),
    (EXEC, "execute_create_index", 1),
    (EXEC, "execute_create_path_index", 1),
    (EXEC, "execute_drop_index", 1),
    (EXEC, "execute_drop_partial_unique", 1),
    (EXEC, "execute_drop_path_index", 1),
    (EXEC, "execute_join", 3),
    (EXEC, "execute_prepared_rows", 1),
    (EXEC, "execute_with_need", 5),
    (EXEC, "existing_rows_break_unique", 1),
    (EXEC, "insert_loaded_row", 1),
    (EXEC, "load_json_document", 1),
    (EXEC, "lock_selected_rows", 1),
    (EXEC, "prevalidate_foreign_keys", 1),
    (EXEC, "query_output_scales", 2),
    (EXEC, "referenced_values_now", 1),
    (EXEC, "refuse_if_partitioned", 1),
    (EXEC, "refuse_write_to_locked_rows", 1),
    (EXEC, "retype_column", 1),
    (EXEC, "rewrite_table_with_columns", 3),
    (EXEC, "star_output_types", 1),
    (EXEC, "supply_generated_values", 1),
    (EXEC, "supply_insert_column_list", 1),
    (EXEC, "update_from_join", 2),
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let path = e.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

/// `src/`'s files, relative with forward slashes, and their text.
fn sources() -> Vec<(String, String)> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    assert!(
        files.len() > 100,
        "found only {} files under {}",
        files.len(),
        src.display()
    );
    files
        .into_iter()
        .map(|path| {
            let rel = path
                .strip_prefix(&src)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let text = std::fs::read_to_string(&path).unwrap();
            (rel, text)
        })
        .collect()
}

/// How many times `name` occurs as an identifier in `tokens`, at any depth.
fn count_ident(tokens: TokenStream, name: &str) -> usize {
    tokens
        .into_iter()
        .map(|t| match t {
            TokenTree::Ident(i) => (i.to_string().trim_start_matches("r#") == name) as usize,
            TokenTree::Group(g) => count_ident(g.stream(), name),
            _ => 0,
        })
        .sum()
}

/// Where `ident` occurs in `file`: each fn (by name, nested fns counted in
/// their own entry, not their parent's) and `"<items>"` for what is outside
/// every fn.
fn occurrences(file: &syn::File, ident: &str) -> BTreeMap<String, usize> {
    struct Walk<'i> {
        ident: &'i str,
        /// (fn name, inclusive count, sum of direct children's inclusive).
        stack: Vec<(String, usize, usize)>,
        out: BTreeMap<String, usize>,
        top: usize,
    }
    impl Walk<'_> {
        fn enter(&mut self, name: String, tokens: TokenStream, inner: impl FnOnce(&mut Self)) {
            let inclusive = count_ident(tokens, self.ident);
            self.stack.push((name, inclusive, 0));
            inner(self);
            let (name, inclusive, children) = self.stack.pop().unwrap();
            match self.stack.last_mut() {
                Some(parent) => parent.2 += inclusive,
                None => self.top += inclusive,
            }
            if inclusive > children {
                *self.out.entry(name).or_default() += inclusive - children;
            }
        }
    }
    impl<'ast> Visit<'ast> for Walk<'_> {
        fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
            self.enter(f.sig.ident.to_string(), f.to_token_stream(), |w| {
                visit::visit_item_fn(w, f)
            });
        }
        fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
            self.enter(f.sig.ident.to_string(), f.to_token_stream(), |w| {
                visit::visit_impl_item_fn(w, f)
            });
        }
        fn visit_trait_item_fn(&mut self, f: &'ast syn::TraitItemFn) {
            self.enter(f.sig.ident.to_string(), f.to_token_stream(), |w| {
                visit::visit_trait_item_fn(w, f)
            });
        }
    }
    let mut walk = Walk {
        ident,
        stack: Vec::new(),
        out: BTreeMap::new(),
        top: 0,
    };
    walk.visit_file(file);
    let total = count_ident(file.to_token_stream(), ident);
    if total > walk.top {
        walk.out.insert("<items>".to_string(), total - walk.top);
    }
    walk.out
}

fn parse(rel: &str, text: &str) -> syn::File {
    syn::parse_file(text).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// Whether the file is test code throughout.
fn is_test_file(rel: &str) -> bool {
    rel.ends_with("tests.rs") || rel.contains("/tests/") || rel.ends_with("test_support.rs")
}

/// Whether `attrs` mark an item as test-only: `#[cfg(test)]` (alone, not
/// under `not`), `#[test]` or `#[tokio::test]`.
fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        let path = a.path();
        if path.is_ident("test") || path.segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        if path.is_ident("cfg") {
            let tokens = a.meta.to_token_stream().to_string().replace(' ', "");
            return tokens == "cfg(test)";
        }
        false
    })
}

/// `file` with its test-only items removed, as tokens.
fn engine_tokens(file: &syn::File) -> TokenStream {
    fn strip_items(items: &mut Vec<syn::Item>) {
        items.retain(|item| {
            let attrs: &[syn::Attribute] = match item {
                syn::Item::Fn(f) => &f.attrs,
                syn::Item::Mod(m) => &m.attrs,
                syn::Item::Impl(i) => &i.attrs,
                syn::Item::Use(u) => &u.attrs,
                syn::Item::Const(c) => &c.attrs,
                syn::Item::Static(s) => &s.attrs,
                syn::Item::Struct(s) => &s.attrs,
                _ => &[],
            };
            !test_only(attrs)
        });
        for item in items.iter_mut() {
            match item {
                syn::Item::Mod(m) => {
                    if let Some((_, inner)) = m.content.as_mut() {
                        strip_items(inner);
                    }
                }
                syn::Item::Impl(i) => i.items.retain(|ii| match ii {
                    syn::ImplItem::Fn(f) => !test_only(&f.attrs),
                    _ => true,
                }),
                _ => {}
            }
        }
    }
    let mut file = file.clone();
    strip_items(&mut file.items);
    file.to_token_stream()
}

/// 1: the maps are named only in `slots.rs`, and where [`MAP_FIELDS`] says.
#[test]
fn the_table_maps_are_named_only_by_the_slot_primitives() {
    let mut seen = Vec::new();
    for (rel, text) in sources() {
        if rel == SLOTS {
            continue;
        }
        for field in ["table_slots", "partitioned_tables"] {
            if !text.contains(field) {
                continue;
            }
            for (at, n) in occurrences(&parse(&rel, &text), field) {
                seen.push((field, rel.clone(), at, n));
            }
        }
    }
    let expected: Vec<(&str, String, String, usize)> = MAP_FIELDS
        .iter()
        .map(|(f, r, a, n)| (*f, r.to_string(), a.to_string(), *n))
        .collect();
    let mut seen_sorted: Vec<_> = seen
        .iter()
        .map(|(f, r, a, n)| (*f, r.clone(), a.clone(), *n))
        .collect();
    seen_sorted.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        seen_sorted, expected_sorted,
        "a table map is named outside the slot primitives (core/database/slots.rs): go through \
         schema_view, relation, has_table or an escape hatch instead"
    );
}

/// 2: the escape hatches' callers are exactly [`ESCAPES`].
#[test]
fn the_escape_hatches_have_exactly_the_listed_callers() {
    let mut seen: Vec<(String, String, usize)> = Vec::new();
    for (rel, text) in sources() {
        if !text.contains("table_unlocked") {
            continue;
        }
        let file = parse(&rel, &text);
        let mut per_fn: BTreeMap<String, usize> = BTreeMap::new();
        for ident in ["table_unlocked", "each_table_unlocked"] {
            for (at, n) in occurrences(&file, ident) {
                *per_fn.entry(at).or_default() += n;
            }
        }
        for (at, n) in per_fn {
            seen.push((rel.clone(), at, n));
        }
    }
    seen.sort();
    let mut expected: Vec<(String, String, usize)> = ESCAPES
        .iter()
        .map(|(r, f, n)| (r.to_string(), f.to_string(), *n))
        .collect();
    expected.sort();
    let listing: String = seen
        .iter()
        .map(|(r, f, n)| format!("    ({r:?}, {f:?}, {n}),\n"))
        .collect();
    assert_eq!(
        seen, expected,
        "the escape hatches' callers changed. A new caller takes a table with no lock: use \
         schema_view for metadata, or (from B3b) table_for. Found:\n{listing}"
    );
}

/// 3: `table_for_tests` is for code outside the engine.
#[test]
fn the_test_accessor_is_never_called_by_the_engine() {
    let mut callers = Vec::new();
    for (rel, text) in sources() {
        if is_test_file(&rel) || !text.contains("table_for_tests") {
            continue;
        }
        let n = count_ident(engine_tokens(&parse(&rel, &text)), "table_for_tests");
        let allowed = (rel == SLOTS) as usize; // its definition
        if n != allowed {
            callers.push((rel, n));
        }
    }
    assert!(
        callers.is_empty(),
        "engine code calls table_for_tests: {callers:?}"
    );
}

/// 4: each fn of `slots.rs` that looks a map up (`get`, `contains_key`)
/// does so through `resolve`, except a walk over the map's own keys.
#[test]
fn every_lookup_by_name_goes_through_resolve() {
    const KEY_WALKS: &[&str] = &["relation_names"];
    fn looks_up(tokens: TokenStream) -> bool {
        let flat: Vec<TokenTree> = tokens.into_iter().collect();
        flat.windows(3).any(|w| match (&w[0], &w[1], &w[2]) {
            (TokenTree::Ident(map), TokenTree::Punct(dot), TokenTree::Ident(op)) => {
                (map == "table_slots" || map == "partitioned_tables")
                    && dot.as_char() == '.'
                    && (op == "get" || op == "contains_key")
            }
            _ => false,
        }) || flat
            .iter()
            .any(|t| matches!(t, TokenTree::Group(g) if looks_up(g.stream())))
    }
    let text = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join(SLOTS),
    )
    .unwrap();
    let file = parse(SLOTS, &text);
    let mut lookups = Vec::new();
    let mut unresolved = Vec::new();
    for item in &file.items {
        let syn::Item::Impl(imp) = item else { continue };
        for ii in &imp.items {
            let syn::ImplItem::Fn(f) = ii else { continue };
            let body = f.block.to_token_stream();
            if !looks_up(body.clone()) {
                continue;
            }
            let name = f.sig.ident.to_string();
            lookups.push(name.clone());
            if !KEY_WALKS.contains(&name.as_str()) && count_ident(body, "resolve") == 0 {
                unresolved.push(name);
            }
        }
    }
    for known in [
        "slot",
        "schema_view",
        "relation",
        "has_table",
        "table_unlocked",
        "partitioned_table",
    ] {
        assert!(
            lookups.iter().any(|l| l == known),
            "the reader missed `{known}`: {lookups:?}"
        );
    }
    assert!(
        unresolved.is_empty(),
        "these look a table name up without `resolve`, so case folding would miss them: {unresolved:?}"
    );
}

/// The readers themselves: a nested fn's use is its own, test-only items
/// are left out, and a lookup without `resolve` is seen.
#[test]
fn the_readers_attribute_uses_to_the_right_fn() {
    let src = r#"
        struct S { table_slots: u8 }
        impl S {
            fn outer(&self) -> u8 {
                fn inner(s: &S) -> u8 { s.table_slots + s.table_slots }
                inner(self) + self.table_slots
            }
        }
        #[cfg(test)]
        mod t { fn f(d: &D) { d.table_for_tests("x"); } }
        #[cfg(not(test))]
        fn g(d: &D) { d.table_for_tests("x"); }
    "#;
    let file = syn::parse_file(src).unwrap();
    let got = occurrences(&file, "table_slots");
    let want: BTreeMap<String, usize> = [
        ("<items>".to_string(), 1),
        ("inner".to_string(), 2),
        ("outer".to_string(), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
    assert_eq!(
        count_ident(engine_tokens(&file), "table_for_tests"),
        1,
        "only g's call is engine code"
    );
}
