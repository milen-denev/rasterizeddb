//! Derive macros for `rasterizeddb_client`; use them through that crate
//! (`rasterizeddb_client::Model` and the rest), which documents them.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, LitStr, Type, parse_macro_input};

/// `#[derive(Model)]`: the struct is a table. See `rasterizeddb_client::orm`.
#[proc_macro_derive(Model, attributes(rdb))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[derive(FromRow)]`: a row read into the struct, column `i` into field
/// `i` (fields marked `#[rdb(skip)]` take their `Default` and no column).
#[proc_macro_derive(FromRow, attributes(rdb))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_row_only(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[derive(SqlEnum)]`: an enum of unit variants stored as text, each
/// variant as its name in snake case (`#[rdb(rename_all = "…")]` for another
/// case: `lowercase`, `UPPERCASE`, `snake_case`, `SCREAMING_SNAKE_CASE`,
/// `kebab-case`, `PascalCase`; `#[rdb(rename = "…")]` on a variant).
#[proc_macro_derive(SqlEnum, attributes(rdb))]
pub fn derive_sql_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    sql_enum(&input).unwrap_or_else(syn::Error::into_compile_error).into()
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
}

fn field_attrs(attrs: &[syn::Attribute]) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("rdb")) {
        attr.parse_nested_meta(|meta| {
            let string = |meta: &syn::meta::ParseNestedMeta<'_>| -> syn::Result<String> {
                Ok(meta.value()?.parse::<LitStr>()?.value())
            };
            let key = meta.path.get_ident().map(|i| i.to_string()).unwrap_or_default();
            match key.as_str() {
                "primary_key" => out.primary_key = true,
                "serial" => out.serial = true,
                "unique" => out.unique = true,
                "json" => out.json = true,
                "text" => out.text = true,
                "skip" => out.skip = true,
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
                _ => {
                    return Err(meta.error(
                        "unknown rdb attribute; expected primary_key, serial, unique, index, \
                         column, sql_type, default, renamed_from, json, text or skip",
                    ));
                }
            }
            Ok(())
        })?;
    }
    if out.skip && (out.primary_key || out.serial || out.unique || out.index.is_some()) {
        return Err(syn::Error::new(Span::call_site(), "a skipped field is not a column"));
    }
    Ok(out)
}

/// The struct-level `#[rdb(key = "…")]` values, for the keys `allowed`;
/// every value of a repeated key, in order.
fn struct_attrs(attrs: &[syn::Attribute], allowed: &[&str]) -> syn::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for attr in attrs.iter().filter(|a| a.path().is_ident("rdb")) {
        attr.parse_nested_meta(|meta| {
            let key = meta.path.get_ident().map(|i| i.to_string()).unwrap_or_default();
            if allowed.contains(&key.as_str()) {
                out.push((key, meta.value()?.parse::<LitStr>()?.value()));
                Ok(())
            } else {
                Err(meta.error(format!("unknown rdb attribute here; expected one of {}", allowed.join(", "))))
            }
        })?;
    }
    Ok(out)
}

fn struct_attr(attrs: &[syn::Attribute], wanted: &str) -> syn::Result<Option<String>> {
    Ok(struct_attrs(attrs, &[wanted])?.into_iter().map(|(_, v)| v).next_back())
}

fn snake_case(name: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let after_lower = i > 0 && (chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit());
            let before_lower = i > 0 && chars.get(i + 1).is_some_and(|n| n.is_lowercase()) && chars[i - 1].is_uppercase();
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
        _ => return Err(syn::Error::new(span, "index is btree, hash, gin or jsonb_path_ops")),
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
        return Err(syn::Error::new(input.span(), "only a struct can be derived"));
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
        return Err(syn::Error::new(input.span(), "a Model needs named fields: they name its columns"));
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
    for f in fields.iter().filter(|f| !f.attrs.skip) {
        let ident = f.ident.expect("named");
        let ty = f.ty;
        let column = f.attrs.column.clone().unwrap_or_else(|| ident.to_string().trim_start_matches("r#").to_string());
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
        let const_name = format_ident!("{}", ident.to_string().trim_start_matches("r#"), span = ident.span());
        let doc = format!("The `{column}` column.");
        consts.push(quote! {
            #[doc = #doc]
            pub const #const_name: ::rasterizeddb_client::orm::Column<Self, #ty> =
                ::rasterizeddb_client::orm::Column::new(#column);
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
        _ => return Err(syn::Error::new(span, "rename_all is snake_case, SCREAMING_SNAKE_CASE, kebab-case, lowercase, UPPERCASE or PascalCase")),
    })
}

fn sql_enum(input: &DeriveInput) -> syn::Result<Tokens> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(input.span(), "SqlEnum is for an enum of unit variants"));
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
