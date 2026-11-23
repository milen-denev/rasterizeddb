//! The fence around rapidhash.
//!
//! GIN items, alone, hash under rapidhash; everything else persisted or
//! routed (shape hashes, the vector history digests, partition routing) is
//! XXH64 and must never move. A stray `ItemFeed::rapid` in one of those
//! would compile, pass its own tests the day it is written, and move every
//! value it persists. So this reads every file under `src/` as tokens (comments
//! and doc text are not tokens; macro bodies are) and checks:
//!
//! 1. The identifier `rapid` (the constructor, however it is reached or
//!    renamed) appears outside `core/hash/` only inside `jsonb.rs`'s
//!    `item_hasher`, exactly once.
//! 2. The identifier `rapidhash` (the module, or its function) appears
//!    nowhere outside `core/hash/`.
//!
//! Both are by name, so an unrelated local called `rapid` fails too: rename
//! it.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

/// Where the hashes live, relative to `src/`, with forward slashes.
const HASH_DIR: &str = "core/hash/";
/// The one fn outside `HASH_DIR` that may build a rapidhash feed.
const RAPID_CALLER: (&str, &str) = ("core/jsonb.rs", "item_hasher");

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

/// The tokens of each fn named `name`, at any depth of `file`'s items.
fn fns_named(file: &syn::File, name: &str) -> Vec<TokenStream> {
    use quote::ToTokens;
    use syn::visit::{self, Visit};
    struct Find<'n>(&'n str, Vec<TokenStream>);
    impl<'ast> Visit<'ast> for Find<'_> {
        fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
            if f.sig.ident == self.0 {
                self.1.push(f.to_token_stream());
            }
            visit::visit_item_fn(self, f);
        }
        fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
            if f.sig.ident == self.0 {
                self.1.push(f.to_token_stream());
            }
            visit::visit_impl_item_fn(self, f);
        }
    }
    let mut find = Find(name, Vec::new());
    find.visit_file(file);
    find.1
}

#[test]
fn rapidhash_is_reached_only_by_gin_items() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    assert!(files.len() > 100, "found only {} files under {}", files.len(), src.display());
    let mut caller_seen = false;
    for path in files {
        let rel = path.strip_prefix(&src).unwrap().to_string_lossy().replace('\\', "/");
        if rel.starts_with(HASH_DIR) {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let tokens = TokenStream::from_str(&text).unwrap_or_else(|e| panic!("{rel}: {e}"));
        assert_eq!(
            count_ident(tokens.clone(), "rapidhash"),
            0,
            "{rel} names `rapidhash`: only core/hash/ may (anything persisted outside GIN items hashes with XXH64)"
        );
        let rapid = count_ident(tokens, "rapid");
        if rel == RAPID_CALLER.0 {
            let file = syn::parse_file(&text).unwrap_or_else(|e| panic!("{rel}: {e}"));
            let callers = fns_named(&file, RAPID_CALLER.1);
            assert_eq!(callers.len(), 1, "{rel} must have exactly one fn {}", RAPID_CALLER.1);
            let inside = count_ident(callers[0].clone(), "rapid");
            assert_eq!(inside, 1, "{rel}'s {} must build its feed with `ItemFeed::rapid`, once", RAPID_CALLER.1);
            assert_eq!(
                rapid,
                inside,
                "{rel} names `rapid` outside {}: GIN items are built only through it",
                RAPID_CALLER.1
            );
            caller_seen = true;
        } else {
            assert_eq!(
                rapid, 0,
                "{rel} names `rapid`: only {}'s {} may build a rapidhash feed; anything else persisted uses ItemFeed::new (XXH64)",
                RAPID_CALLER.0, RAPID_CALLER.1
            );
        }
    }
    assert!(caller_seen, "{} not found", RAPID_CALLER.0);
}
