//! Derive macros for `rasterizeddb_client`; use them through that crate
//! (`rasterizeddb_client::Model` and the rest), which documents them.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, LitStr, Type, parse_macro_input};

/// The struct is a table.
///
/// Implements `Model` (the table's name, its columns, and how each field is
/// written) and `FromRow` (reading the model's select list), and gives the
/// struct one associated constant per column, named as the field and typed
/// `Column<Self, FieldType>`, for the query builder: `Account::email.eq("…")`.
///
/// ```ignore
/// use rasterizeddb_client::prelude::*;
///
/// #[derive(Model, Debug, Clone)]
/// #[rdb(table = "accounts")]
/// struct Account {
///     #[rdb(primary_key, serial)]
///     id: i64,
///     #[rdb(unique)]
///     email: String,
///     #[rdb(index = "hash", renamed_from = "full_name")]
///     name: Option<String>,
///     balance: Numeric,
///     #[rdb(skip)]
///     cached: Option<u32>,
/// }
/// ```
///
/// # Field attributes
///
/// | attribute | meaning |
/// |---|---|
/// | `primary_key` | part of the primary key; several fields make a composite key |
/// | `serial` | assigned by the server's sequence: left out of inserts, read back |
/// | `unique` | a `UNIQUE` constraint |
/// | `index`, `index = "hash"` | a secondary index: `btree` (default), `hash`, `gin`, `jsonb_path_ops` |
/// | `column = "name"` | the column's name, when it is not the field's |
/// | `sql_type = "VARCHAR(64)"` | the DDL type, when the field type's default is not wanted |
/// | `default = "…"` | a `DEFAULT` expression for the DDL |
/// | `renamed_from = "old"` | an earlier column name, for the migrator to rename (may repeat) |
/// | `json` | any `Serialize + Deserialize` type, stored as `jsonb` (feature `serde_json`) |
/// | `text` | read as `col::text` and parsed (for a type of your own) |
/// | `skip` | not a column: `Default::default()` on reads |
/// | `version` | an integer for optimistic locking: inserts write 1, updates `version + 1` where it is still the struct's; a lost race is `Error::Conflict` |
/// | `references = "users(id)"` | a foreign key in the DDL, with `on_delete = "cascade"` / `on_update = …` |
///
/// # Relation fields
///
/// Not columns: each gets a `Relation<Self, C>` constant named as the field,
/// and is filled by loading it (`Default` until then).
///
/// | attribute | field type |
/// |---|---|
/// | `has_many(Post, foreign_key = "user_id")` | `Vec<Post>` |
/// | `has_one(Profile, foreign_key = "user_id")` | `Option<Profile>` |
/// | `belongs_to(User, foreign_key = "user_id")` | `Option<User>` or `Option<Box<User>>` |
/// | `many_to_many(Tag, through = "user_tags", local = "user_id", foreign = "tag_id")` | `Vec<Tag>` |
/// | `count_of = "posts"` | `i64`, filled by `with_count` |
///
/// `local_key` (has_many, has_one, many_to_many), `owner_key` (belongs_to)
/// and `related_key` (many_to_many) name the matched column when it is not
/// that model's primary key; `through` takes a join model's type too.
///
/// # Struct attributes
///
/// | attribute | meaning |
/// |---|---|
/// | `table = "name"` | the table's name (otherwise the struct's in snake case) |
/// | `renamed_from = "old"` | an earlier table name, for the migrator to rename |
///
/// A field's type must implement `SqlType` (every type the client maps does)
/// unless it has `sql_type` or `json`.
#[proc_macro_derive(Model, attributes(rdb))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// A row read into the struct by position: column `i` into field `i`.
///
/// For the result of a statement you write yourself; a `Model` derives this
/// already. A row with more or fewer columns than the struct has fields is an
/// error. Field attributes: `skip` (no column; `Default::default()`) and
/// `json` (the column holds JSON text, deserialized; feature `serde_json`).
///
/// ```ignore
/// #[derive(FromRow)]
/// struct Total { customer: String, orders: i64, spent: Numeric }
///
/// let totals: Vec<Total> = client
///     .fetch_all("SELECT customer, COUNT(*), SUM(total)::text FROM orders GROUP BY customer", params![])
///     .await?;
/// ```
#[proc_macro_derive(FromRow, attributes(rdb))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_row_only(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// An enum of unit variants stored as text.
///
/// Each variant is stored as its name in snake case. `#[rdb(rename_all =
/// "…")]` on the enum picks another case (`lowercase`, `UPPERCASE`,
/// `snake_case`, `SCREAMING_SNAKE_CASE`, `kebab-case`, `PascalCase`), and
/// `#[rdb(rename = "…")]` on a variant names it outright. Implements `ToSql`,
/// `FromSql` (an unknown text is an error naming the variants) and `SqlType`
/// (`TEXT`), and adds `as_sql_str()`.
///
/// ```ignore
/// #[derive(SqlEnum, Clone, Copy, PartialEq, Debug)]
/// enum Plan { Free, Pro, #[rdb(rename = "enterprise-2026")] Enterprise }
/// ```
#[proc_macro_derive(SqlEnum, attributes(rdb))]
pub fn derive_sql_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    sql_enum(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

// ----------------------------------------------------------------- attributes

#[derive(Default)]
struct FieldAttrs {
    primary_key: bool,
    serial: bool,
    unique: bool,
    index: Option<String>,
    column: Option<String>,
    sql_type: Option<String>,
    default: Option<String>,
    json: bool,
    text: bool,
    skip: bool,
    renamed_from: Vec<String>,
    /// `version`, …: what the ORM maintains in the column.
    role: Option<(&'static str, Span)>,
    /// `references = "table(column)"`, with its actions.
    references: Option<(String, String)>,
    on_delete: Option<String>,
    on_update: Option<String>,
    /// A relation field: not a column.
    relation: Option<RelationAttr>,
    /// `count_of = "relation"`: not a column; holds a relation's count.
    count_of: Option<(String, Span)>,
}

struct RelationAttr {
    /// `BelongsTo`, `HasOne`, `HasMany`, `ManyToMany`.
    kind: &'static str,
    target: Option<syn::Path>,
    foreign_key: Option<String>,
    local_key: Option<String>,
    owner_key: Option<String>,
    related_key: Option<String>,
    through: Option<Tokens>,
    local: Option<String>,
    foreign: Option<String>,
    span: Span,
}

fn relation_attr(
    meta: &syn::meta::ParseNestedMeta<'_>,
    kind: &'static str,
) -> syn::Result<RelationAttr> {
    let mut out = RelationAttr {
        kind,
        target: None,
        foreign_key: None,
        local_key: None,
        owner_key: None,
        related_key: None,
        through: None,
        local: None,
        foreign: None,
        span: meta.path.span(),
    };
    meta.parse_nested_meta(|inner| {
        if !inner.input.peek(syn::Token![=]) {
            if out.target.is_some() {
                return Err(inner.error("a relation names one model"));
            }
            out.target = Some(inner.path.clone());
            return Ok(());
        }
        let key = inner
            .path
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        let value = inner.value()?;
        if key == "through" {
            out.through = Some(if value.peek(LitStr) {
                let table = value.parse::<LitStr>()?.value();
                quote!(#table)
            } else {
                let model = value.parse::<syn::Path>()?;
                quote!(<#model as ::rasterizeddb_client::orm::Model>::TABLE)
            });
            return Ok(());
        }
        let text = value.parse::<LitStr>()?.value();
        let slot = match key.as_str() {
            "foreign_key" => &mut out.foreign_key,
            "local_key" => &mut out.local_key,
            "owner_key" => &mut out.owner_key,
            "related_key" => &mut out.related_key,
            "local" => &mut out.local,
            "foreign" => &mut out.foreign,
            _ => {
                return Err(inner.error(
                    "expected the related model, then foreign_key, local_key, owner_key, \
                     related_key, through, local or foreign",
                ));
            }
        };
        *slot = Some(text);
        Ok(())
    })?;
    if out.target.is_none() {
        return Err(
            meta.error("name the related model first: has_many(Post, foreign_key = \"user_id\")")
        );
    }
    let wrong = |what: &str| Err(syn::Error::new(out.span, what.to_string()));
    match kind {
        "HasMany" | "HasOne" => {
            if out.foreign_key.is_none() {
                return wrong(
                    "has_many/has_one need foreign_key = \"<column of the related model>\"",
                );
            }
            if out.owner_key.is_some() || out.related_key.is_some() || out.through.is_some() {
                return wrong("has_many/has_one take foreign_key and local_key");
            }
        }
        "BelongsTo" => {
            if out.foreign_key.is_none() {
                return wrong("belongs_to needs foreign_key = \"<column of this model>\"");
            }
            if out.local_key.is_some() || out.related_key.is_some() || out.through.is_some() {
                return wrong("belongs_to takes foreign_key and owner_key");
            }
        }
        _ => {
            if out.through.is_none() || out.local.is_none() || out.foreign.is_none() {
                return wrong(
                    "many_to_many needs through = \"<join table>\" (or a join model), \
                     local = \"<its column for this model>\" and foreign = \"<its column for the related one>\"",
                );
            }
            if out.foreign_key.is_some() || out.owner_key.is_some() {
                return wrong(
                    "many_to_many takes through, local, foreign, local_key and related_key",
                );
            }
        }
    }
    Ok(out)
}

fn fk_action(text: &str, span: Span) -> syn::Result<String> {
    let upper = text.trim().to_ascii_uppercase();
    match upper.as_str() {
        "CASCADE" | "RESTRICT" | "SET NULL" | "SET DEFAULT" | "NO ACTION" => Ok(upper),
        _ => Err(syn::Error::new(
            span,
            "a foreign key action is cascade, restrict, set null, set default or no action",
        )),
    }
}

fn field_attrs(attrs: &[syn::Attribute]) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("rdb")) {
        attr.parse_nested_meta(|meta| {
            let string = |meta: &syn::meta::ParseNestedMeta<'_>| -> syn::Result<String> {
                Ok(meta.value()?.parse::<LitStr>()?.value())
            };
            let key = meta
                .path
                .get_ident()
                .map(|i| i.to_string())
                .unwrap_or_default();
            match key.as_str() {
                "primary_key" => out.primary_key = true,
                "serial" => out.serial = true,
                "unique" => out.unique = true,
                "json" => out.json = true,
                "text" => out.text = true,
                "skip" => out.skip = true,
                "version" => {
                    if out.role.is_some() {
                        return Err(meta.error("a column has one maintained role"));
                    }
                    out.role = Some(("Version", meta.path.span()));
                }
                "index" => {
                    out.index = Some(if meta.input.peek(syn::Token![=]) {
                        string(&meta)?
                    } else {
                        "btree".to_string()
                    })
                }
                "column" | "rename" => out.column = Some(string(&meta)?),
                "sql_type" => out.sql_type = Some(string(&meta)?),
                "default" => out.default = Some(string(&meta)?),
                "renamed_from" => out.renamed_from.push(string(&meta)?),
                "references" => {
                    let text = string(&meta)?;
                    let parsed = text.trim().strip_suffix(')').and_then(|t| {
                        let (table, column) = t.split_once('(')?;
                        let (table, column) = (table.trim(), column.trim());
                        (!table.is_empty() && !column.is_empty() && !column.contains(','))
                            .then(|| (table.to_string(), column.to_string()))
                    });
                    match parsed {
                        Some(pair) => out.references = Some(pair),
                        None => {
                            return Err(
                                meta.error("references = \"table(column)\": one referenced column")
                            );
                        }
                    }
                }
                "on_delete" => out.on_delete = Some(fk_action(&string(&meta)?, meta.path.span())?),
                "on_update" => out.on_update = Some(fk_action(&string(&meta)?, meta.path.span())?),
                "has_many" => out.relation = Some(relation_attr(&meta, "HasMany")?),
                "has_one" => out.relation = Some(relation_attr(&meta, "HasOne")?),
                "belongs_to" => out.relation = Some(relation_attr(&meta, "BelongsTo")?),
                "many_to_many" => out.relation = Some(relation_attr(&meta, "ManyToMany")?),
                "count_of" => out.count_of = Some((string(&meta)?, meta.path.span())),
                _ => {
                    return Err(meta.error(
                        "unknown rdb attribute; expected primary_key, serial, unique, index, \
                         column, sql_type, default, renamed_from, references, on_delete, \
                         on_update, json, text, skip, version, has_many, has_one, belongs_to, \
                         many_to_many or count_of",
                    ));
                }
            }
            Ok(())
        })?;
    }
    if (out.on_delete.is_some() || out.on_update.is_some()) && out.references.is_none() {
        return Err(syn::Error::new(
            Span::call_site(),
            "on_delete and on_update go with references = \"table(column)\"",
        ));
    }
    if out.relation.is_some() || out.count_of.is_some() {
        if out.relation.is_some() && out.count_of.is_some() {
            return Err(syn::Error::new(
                Span::call_site(),
                "a field is a relation or a count_of, not both",
            ));
        }
        if out.skip {
            return Err(syn::Error::new(
                Span::call_site(),
                "a relation or count_of field is not a column already; drop skip",
            ));
        }
        // Not a column: read as Default, filled by loading.
        out.skip = true;
        if out.primary_key
            || out.serial
            || out.unique
            || out.index.is_some()
            || out.role.is_some()
            || out.references.is_some()
            || out.column.is_some()
            || out.sql_type.is_some()
            || out.default.is_some()
            || out.json
            || out.text
        {
            return Err(syn::Error::new(
                Span::call_site(),
                "a relation or count_of field is not a column: it takes no column attributes",
            ));
        }
        return Ok(out);
    }
    if out.skip
        && (out.primary_key
            || out.serial
            || out.unique
            || out.index.is_some()
            || out.role.is_some())
    {
        return Err(syn::Error::new(
            Span::call_site(),
            "a skipped field is not a column",
        ));
    }
    if let Some((role, span)) = out.role
        && (out.primary_key || out.serial)
    {
        return Err(syn::Error::new(
            span,
            format!(
                "a {} column is maintained by the ORM, so it is not a key or serial",
                role.to_lowercase()
            ),
        ));
    }
    Ok(out)
}

/// The struct-level `#[rdb(key = "…")]` values, for the keys `allowed`;
/// every value of a repeated key, in order.
fn struct_attrs(attrs: &[syn::Attribute], allowed: &[&str]) -> syn::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("rdb")) {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(|i| i.to_string())
                .unwrap_or_default();
            if allowed.contains(&key.as_str()) {
                out.push((key, meta.value()?.parse::<LitStr>()?.value()));
                Ok(())
            } else {
                Err(meta.error(format!(
                    "unknown rdb attribute here; expected one of {}",
                    allowed.join(", ")
                )))
            }
        })?;
    }
    Ok(out)
}

fn struct_attr(attrs: &[syn::Attribute], wanted: &str) -> syn::Result<Option<String>> {
    Ok(struct_attrs(attrs, &[wanted])?
        .into_iter()
        .map(|(_, v)| v)
        .next_back())
}

fn snake_case(name: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let after_lower =
                i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let before_lower = i > 0
                && chars.get(i + 1).is_some_and(|n| n.is_lowercase())
                && chars[i - 1].is_uppercase();
            if after_lower || before_lower {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether the type is spelled `Option<…>`: nullable in the DDL.
fn is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(p) => p.path.segments.last().is_some_and(|s| s.ident == "Option"),
        _ => false,
    }
}

fn index_method(name: &str, span: Span) -> syn::Result<Tokens> {
    let variant = match name.to_ascii_lowercase().as_str() {
        "btree" | "b-tree" => quote!(BTree),
        "hash" => quote!(Hash),
        "gin" => quote!(Gin),
        "gin_path" | "jsonb_path_ops" => quote!(GinPathOps),
        _ => {
            return Err(syn::Error::new(
                span,
                "index is btree, hash, gin or jsonb_path_ops",
            ));
        }
    };
    Ok(quote!(::rasterizeddb_client::admin::IndexMethod::#variant))
}

// ---------------------------------------------------------------------- Model

struct Field<'a> {
    access: Tokens,
    ident: Option<&'a syn::Ident>,
    ty: &'a Type,
    attrs: FieldAttrs,
}

fn fields(input: &DeriveInput) -> syn::Result<(Vec<Field<'_>>, bool)> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "only a struct can be derived",
        ));
    };
    let named = matches!(data.fields, Fields::Named(_));
    let mut out = Vec::new();
    for (i, f) in data.fields.iter().enumerate() {
        let access = match &f.ident {
            Some(ident) => quote!(#ident),
            None => {
                let index = syn::Index::from(i);
                quote!(#index)
            }
        };
        out.push(Field {
            access,
            ident: f.ident.as_ref(),
            ty: &f.ty,
            attrs: field_attrs(&f.attrs)?,
        });
    }
    Ok((out, named))
}

/// `FromRow`: each column field read by position.
fn from_row_impl(input: &DeriveInput, fields: &[Field<'_>], named: bool) -> Tokens {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let width = fields.iter().filter(|f| !f.attrs.skip).count();
    let mut column = 0usize;
    let values: Vec<Tokens> = fields
        .iter()
        .map(|f| {
            let ty = f.ty;
            let value = if f.attrs.skip {
                quote!(::core::default::Default::default())
            } else {
                let i = column;
                column += 1;
                if f.attrs.json {
                    quote!(::rasterizeddb_client::__private::json_from_row::<#ty>(row, #i)?)
                } else {
                    quote!(row.get::<#ty>(#i)?)
                }
            };
            match f.ident {
                Some(ident) => quote!(#ident: #value),
                None => value,
            }
        })
        .collect();
    let body = if named {
        quote!(#name { #(#values),* })
    } else {
        quote!(#name ( #(#values),* ))
    };
    quote! {
        impl #impl_generics ::rasterizeddb_client::FromRow for #name #ty_generics #where_clause {
            fn from_row(row: &::rasterizeddb_client::Row<'_>) -> ::rasterizeddb_client::Result<Self> {
                ::rasterizeddb_client::__private::check_width(row, #width)?;
                ::core::result::Result::Ok(#body)
            }
        }
    }
}

fn from_row_only(input: &DeriveInput) -> syn::Result<Tokens> {
    let (fields, named) = fields(input)?;
    Ok(from_row_impl(input, &fields, named))
}

fn model(input: &DeriveInput) -> syn::Result<Tokens> {
    let (fields, named) = fields(input)?;
    if !named {
        return Err(syn::Error::new(
            input.span(),
            "a Model needs named fields: they name its columns",
        ));
    }
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let struct_settings = struct_attrs(&input.attrs, &["table", "renamed_from"])?;
    let table = struct_settings
        .iter()
        .filter(|(k, _)| k == "table")
        .map(|(_, v)| v.clone())
        .next_back()
        .unwrap_or_else(|| snake_case(&name.to_string()));
    let table_renamed_from: Vec<&String> = struct_settings
        .iter()
        .filter(|(k, _)| k == "renamed_from")
        .map(|(_, v)| v)
        .collect();

    let mut defs = Vec::new();
    let mut writes = Vec::new();
    let mut texts = Vec::new();
    let mut consts = Vec::new();
    let mut roles: Vec<&'static str> = Vec::new();
    for f in fields.iter().filter(|f| !f.attrs.skip) {
        let ident = f.ident.expect("named");
        let ty = f.ty;
        let column = f
            .attrs
            .column
            .clone()
            .unwrap_or_else(|| ident.to_string().trim_start_matches("r#").to_string());
        let i = defs.len();
        let nullable = is_option(ty);
        let sql_type = match (&f.attrs.sql_type, f.attrs.json) {
            (Some(t), _) => quote!(#t),
            (None, true) => quote!("JSONB"),
            (None, false) => quote!(<#ty as ::rasterizeddb_client::orm::SqlType>::SQL_TYPE),
        };
        let read_as_text = if f.attrs.json || f.attrs.text {
            quote!(true)
        } else if f.attrs.sql_type.is_some() {
            quote!(false)
        } else {
            quote!(<#ty as ::rasterizeddb_client::orm::SqlType>::READ_AS_TEXT)
        };
        let index = match &f.attrs.index {
            Some(method) => {
                let method = index_method(method, ident.span())?;
                quote!(::core::option::Option::Some(#method))
            }
            None => quote!(::core::option::Option::None),
        };
        let default = match &f.attrs.default {
            Some(d) => quote!(::core::option::Option::Some(#d)),
            None => quote!(::core::option::Option::None),
        };
        let (primary_key, serial, unique) = (f.attrs.primary_key, f.attrs.serial, f.attrs.unique);
        let renamed_from = &f.attrs.renamed_from;
        let references = match &f.attrs.references {
            Some((table, column)) => {
                let action = |a: &Option<String>| match a {
                    Some(a) => quote!(::core::option::Option::Some(#a)),
                    None => quote!(::core::option::Option::None),
                };
                let (on_delete, on_update) =
                    (action(&f.attrs.on_delete), action(&f.attrs.on_update));
                quote!(::core::option::Option::Some(::rasterizeddb_client::orm::ForeignKey {
                    table: #table,
                    column: #column,
                    on_delete: #on_delete,
                    on_update: #on_update,
                }))
            }
            None => quote!(::core::option::Option::None),
        };
        let role = match f.attrs.role {
            Some((role, span)) => {
                if roles.contains(&role) {
                    return Err(syn::Error::new(
                        span,
                        format!("a model has one {} column", role.to_lowercase()),
                    ));
                }
                roles.push(role);
                let variant = format_ident!("{}", role);
                quote!(::rasterizeddb_client::orm::ColumnRole::#variant)
            }
            None => quote!(::rasterizeddb_client::orm::ColumnRole::Plain),
        };
        defs.push(quote! {
            ::rasterizeddb_client::orm::ColumnDef {
                name: #column,
                sql_type: #sql_type,
                nullable: #nullable,
                primary_key: #primary_key,
                serial: #serial,
                unique: #unique,
                index: #index,
                default: #default,
                read_as_text: #read_as_text,
                renamed_from: &[#(#renamed_from),*],
                role: #role,
                references: #references,
            }
        });
        let access = &f.access;
        let write = if f.attrs.json && nullable {
            quote! {
                match &self.#access {
                    ::core::option::Option::None => { out.push_str("NULL"); ::core::result::Result::Ok(()) }
                    ::core::option::Option::Some(v) => ::rasterizeddb_client::__private::json_param(v, out),
                }
            }
        } else if f.attrs.json {
            quote!(::rasterizeddb_client::__private::json_param(&self.#access, out))
        } else {
            quote! {
                ::rasterizeddb_client::__private::push_param(
                    out,
                    &::rasterizeddb_client::ToSql::to_param(&self.#access),
                )
            }
        };
        writes.push(quote!(#i => #write,));
        let text = if f.attrs.json && nullable {
            quote! {
                match &self.#access {
                    ::core::option::Option::None => ::core::result::Result::Ok(false),
                    ::core::option::Option::Some(v) => ::rasterizeddb_client::__private::json_text(v, out),
                }
            }
        } else if f.attrs.json {
            quote!(::rasterizeddb_client::__private::json_text(&self.#access, out))
        } else {
            quote! {
                ::rasterizeddb_client::__private::push_text_form(
                    out,
                    &::rasterizeddb_client::ToSql::to_param(&self.#access),
                )
            }
        };
        texts.push(quote!(#i => #text,));
        let const_name = format_ident!(
            "{}",
            ident.to_string().trim_start_matches("r#"),
            span = ident.span()
        );
        let doc = format!("The `{column}` column.");
        consts.push(quote! {
            #[doc = #doc]
            pub const #const_name: ::rasterizeddb_client::orm::Column<Self, #ty> =
                ::rasterizeddb_client::orm::Column::new(#column);
        });
    }
    // Relation fields: a `Relation<Self, C>` constant each.
    for f in fields.iter().filter(|f| f.attrs.count_of.is_some()) {
        let (target, span) = f.attrs.count_of.as_ref().expect("filtered");
        let named = fields
            .iter()
            .any(|r| r.attrs.relation.is_some() && r.ident.is_some_and(|i| i == target));
        if !named {
            return Err(syn::Error::new(
                *span,
                format!("count_of = \"{target}\" names no relation field of this struct"),
            ));
        }
    }
    for f in fields.iter() {
        let Some(relation) = &f.attrs.relation else {
            continue;
        };
        let ident = f.ident.expect("named");
        let field_name = ident.to_string().trim_start_matches("r#").to_string();
        let target = relation.target.as_ref().expect("checked");
        let kind = format_ident!("{}", relation.kind);
        let opt = |v: &Option<String>| match v {
            Some(v) => quote!(::core::option::Option::Some(#v)),
            None => quote!(::core::option::Option::None),
        };
        let (parent_key, child_key) = match relation.kind {
            "BelongsTo" => (opt(&relation.foreign_key), opt(&relation.owner_key)),
            "ManyToMany" => (opt(&relation.local_key), opt(&relation.related_key)),
            _ => (opt(&relation.local_key), opt(&relation.foreign_key)),
        };
        let through = match &relation.through {
            Some(table) => {
                let (local, foreign) = (
                    relation.local.as_ref().expect("checked"),
                    relation.foreign.as_ref().expect("checked"),
                );
                quote!(::core::option::Option::Some(::rasterizeddb_client::orm::Through {
                    table: #table,
                    local: #local,
                    foreign: #foreign,
                }))
            }
            None => quote!(::core::option::Option::None),
        };
        let count = fields.iter().find(|c| {
            c.attrs
                .count_of
                .as_ref()
                .is_some_and(|(t, _)| *t == field_name)
        });
        let count = match count {
            Some(c) => {
                let access = &c.access;
                quote!(::core::option::Option::Some(|p: &mut Self, n: i64| p.#access = n))
            }
            None => quote!(::core::option::Option::None),
        };
        let const_name = format_ident!("{}", field_name, span = ident.span());
        let doc = format!(
            "The `{field_name}` relation: {} `{}`.",
            match relation.kind {
                "BelongsTo" => "belongs to",
                "HasOne" => "has one",
                "HasMany" => "has many",
                _ => "many to many,",
            },
            quote!(#target).to_string().replace(' ', "")
        );
        let access = &f.access;
        consts.push(quote! {
            #[doc = #doc]
            pub const #const_name: ::rasterizeddb_client::orm::Relation<Self, #target> =
                ::rasterizeddb_client::orm::Relation::new(
                    #field_name,
                    ::rasterizeddb_client::orm::RelationKind::#kind,
                    #parent_key,
                    #child_key,
                    #through,
                    |p: &mut Self, rows: ::std::vec::Vec<#target>| {
                        ::rasterizeddb_client::orm::RelationSlot::<#target>::fill(&mut p.#access, rows)
                    },
                    #count,
                );
        });
    }
    let count = defs.len();
    let from_row = from_row_impl(input, &fields, named);
    Ok(quote! {
        impl #impl_generics ::rasterizeddb_client::orm::Model for #name #ty_generics #where_clause {
            const TABLE: &'static str = #table;
            const COLUMNS: &'static [::rasterizeddb_client::orm::ColumnDef] = &[#(#defs),*];
            const RENAMED_FROM: &'static [&'static str] = &[#(#table_renamed_from),*];

            fn write_value(&self, column: usize, out: &mut ::std::string::String) -> ::rasterizeddb_client::Result<()> {
                match column {
                    #(#writes)*
                    other => ::core::result::Result::Err(::rasterizeddb_client::Error::Param(
                        ::std::format!("{} has {} columns, not {}", #table, #count, other + 1),
                    )),
                }
            }

            fn write_text(&self, column: usize, out: &mut ::std::vec::Vec<u8>) -> ::rasterizeddb_client::Result<bool> {
                match column {
                    #(#texts)*
                    other => ::core::result::Result::Err(::rasterizeddb_client::Error::Param(
                        ::std::format!("{} has {} columns, not {}", #table, #count, other + 1),
                    )),
                }
            }
        }

        #[allow(non_upper_case_globals)]
        impl #impl_generics #name #ty_generics #where_clause {
            #(#consts)*
        }

        #from_row
    })
}

// -------------------------------------------------------------------- SqlEnum

fn rename(name: &str, rule: &str, span: Span) -> syn::Result<String> {
    let snake = snake_case(name);
    Ok(match rule {
        "snake_case" => snake,
        "SCREAMING_SNAKE_CASE" => snake.to_uppercase(),
        "kebab-case" => snake.replace('_', "-"),
        "lowercase" => name.to_lowercase(),
        "UPPERCASE" => name.to_uppercase(),
        "PascalCase" => name.to_string(),
        _ => {
            return Err(syn::Error::new(
                span,
                "rename_all is snake_case, SCREAMING_SNAKE_CASE, kebab-case, lowercase, UPPERCASE or PascalCase",
            ));
        }
    })
}

fn sql_enum(input: &DeriveInput) -> syn::Result<Tokens> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "SqlEnum is for an enum of unit variants",
        ));
    };
    let name = &input.ident;
    let rule = struct_attr(&input.attrs, "rename_all")?.unwrap_or_else(|| "snake_case".to_string());
    let mut to = Vec::new();
    let mut from = Vec::new();
    let mut names = Vec::new();
    for v in &data.variants {
        if !matches!(v.fields, Fields::Unit) {
            return Err(syn::Error::new(v.span(), "SqlEnum variants carry no data"));
        }
        let mut text = rename(&v.ident.to_string(), &rule, v.span())?;
        for attr in v.attrs.iter().filter(|a| a.path().is_ident("rdb")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    text = meta.value()?.parse::<LitStr>()?.value();
                    Ok(())
                } else {
                    Err(meta.error("expected rename"))
                }
            })?;
        }
        let ident = &v.ident;
        to.push(quote!(#name::#ident => #text,));
        from.push(quote!(#text => ::core::result::Result::Ok(#name::#ident),));
        names.push(text);
    }
    let expected = names.join(", ");
    Ok(quote! {
        impl #name {
            /// The text this variant is stored as.
            pub const fn as_sql_str(&self) -> &'static str {
                match self { #(#to)* }
            }
        }

        impl ::rasterizeddb_client::ToSql for #name {
            fn to_param(&self) -> ::rasterizeddb_client::Param<'_> {
                ::rasterizeddb_client::Param::Text(self.as_sql_str())
            }
        }

        impl ::rasterizeddb_client::FromSql<'_> for #name {
            fn from_sql(tag: u8, raw: &[u8]) -> ::core::result::Result<Self, ::std::string::String> {
                let text = <&str as ::rasterizeddb_client::FromSql<'_>>::from_sql(tag, raw)?;
                match text {
                    #(#from)*
                    other => ::core::result::Result::Err(::std::format!(
                        "{:?} is not a {}: expected one of {}", other, ::core::stringify!(#name), #expected
                    )),
                }
            }
        }

        impl ::rasterizeddb_client::orm::SqlType for #name {
            const SQL_TYPE: &'static str = "TEXT";
        }
    })
}
