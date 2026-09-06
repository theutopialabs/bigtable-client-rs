//! Derive support for `bigtable-client`.
//!
//! This proc-macro crate has no runtime configuration. Invalid attributes are
//! reported as compiler diagnostics.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Field, Fields, GenericArgument, Generics, Lit,
    LitByteStr, LitStr, Path, PathArguments, Type, ext::IdentExt, parse_macro_input, parse_quote,
    spanned::Spanned,
};

/// Derives `bigtable_client::FromRow` for a named struct.
///
/// Set a default family with `#[bigtable(family = "profile")]`. Fields use
/// their Rust name as the qualifier unless `qualifier` overrides it.
///
/// Supported field attributes are:
///
/// - `row_key`
/// - `family = "name"`
/// - `qualifier = "name"` or `qualifier = b"\xff"`
/// - `json`
/// - `with = "decoder_path"`
/// - `default`
///
/// An `Option<T>` field is sparse. A plain field is required and selects the
/// latest visible cell.
#[proc_macro_derive(FromRow, attributes(bigtable))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_from_row(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_from_row(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let crate_path = client_crate();
    let struct_config = StructConfig::parse(&input.attrs)?;
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(syn::Error::new(
                    data.fields.span(),
                    "FromRow can only be derived for a struct with named fields",
                ));
            }
        },
        Data::Enum(data) => {
            return Err(syn::Error::new(
                data.enum_token.span,
                "FromRow cannot be derived for an enum",
            ));
        }
        Data::Union(data) => {
            return Err(syn::Error::new(
                data.union_token.span,
                "FromRow cannot be derived for a union",
            ));
        }
    };

    let mut generics = input.generics.clone();
    let mut initializers = Vec::with_capacity(fields.len());
    let mut row_key_field: Option<Span> = None;

    for field in fields {
        let field_config = FieldConfig::parse(field)?;
        if field_config.row_key {
            if let Some(first_span) = row_key_field {
                let mut error =
                    syn::Error::new(field.span(), "only one field can use #[bigtable(row_key)]");
                error.combine(syn::Error::new(first_span, "first row key field is here"));
                return Err(error);
            }
            row_key_field = Some(field.span());
        }
        initializers.push(field_initializer(
            field,
            &field_config,
            &struct_config,
            &crate_path,
            &mut generics,
        )?);
    }

    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #crate_path::FromRow for #name #type_generics #where_clause {
            fn from_row(
                row: #crate_path::Row,
            ) -> ::core::result::Result<Self, #crate_path::RowMappingError> {
                let decoder = #crate_path::RowDecoder::new(&row);
                ::core::result::Result::Ok(Self {
                    #(#initializers),*
                })
            }
        }
    })
}

fn field_initializer(
    field: &Field,
    config: &FieldConfig,
    struct_config: &StructConfig,
    crate_path: &TokenStream2,
    generics: &mut Generics,
) -> syn::Result<TokenStream2> {
    let ident = field
        .ident
        .as_ref()
        .ok_or_else(|| syn::Error::new(field.span(), "FromRow requires named fields"))?;
    let option_inner = option_inner(&field.ty);

    if config.row_key {
        validate_row_key_config(field, config, option_inner)?;
        add_decoder_bound(generics, &field.ty, config, crate_path);
        let decode = row_key_decoder(config, &field.ty);
        return Ok(quote!(#ident: #decode));
    }

    let family = config
        .family
        .as_ref()
        .or(struct_config.family.as_ref())
        .ok_or_else(|| {
            syn::Error::new(
                field.span(),
                "column fields need #[bigtable(family = \"...\")] on the struct or field",
            )
        })?;
    let qualifier = config
        .qualifier
        .clone()
        .unwrap_or_else(|| Qualifier::new(ident.unraw().to_string().as_bytes(), ident.span()));
    let qualifier = qualifier.literal();

    if option_inner.is_some() && config.default {
        return Err(syn::Error::new(
            field.span(),
            "Option fields are already sparse and cannot also use #[bigtable(default)]",
        ));
    }

    let target_type = option_inner.unwrap_or(&field.ty);
    add_decoder_bound(generics, target_type, config, crate_path);
    if config.default {
        let default_bound = parse_quote!(::core::default::Default);
        add_bound(generics, target_type, &default_bound);
    }

    let optional = option_inner.is_some() || config.default;
    let (plain_method, json_method, with_method) = if optional {
        (
            quote!(decoder.optional),
            quote!(decoder.optional_json),
            quote!(decoder.optional_with),
        )
    } else {
        (
            quote!(decoder.required),
            quote!(decoder.required_json),
            quote!(decoder.required_with),
        )
    };
    let mut decode = if config.json {
        quote!(#json_method::<#target_type>(#family, #qualifier)?)
    } else if let Some(path) = &config.with {
        quote!(#with_method(#family, #qualifier, #path)?)
    } else {
        quote!(#plain_method::<#target_type>(#family, #qualifier)?)
    };
    if config.default {
        decode = quote!(#decode.unwrap_or_default());
    }

    Ok(quote!(#ident: #decode))
}

fn row_key_decoder(config: &FieldConfig, target_type: &Type) -> TokenStream2 {
    if config.json {
        quote!(decoder.row_key_json::<#target_type>()?)
    } else if let Some(path) = &config.with {
        quote!(decoder.row_key_with(#path)?)
    } else {
        quote!(decoder.row_key::<#target_type>()?)
    }
}

fn validate_row_key_config(
    field: &Field,
    config: &FieldConfig,
    option_inner: Option<&Type>,
) -> syn::Result<()> {
    if config.family.is_some() || config.qualifier.is_some() {
        return Err(syn::Error::new(
            field.span(),
            "a row key field cannot select a family or qualifier",
        ));
    }
    if option_inner.is_some() {
        return Err(syn::Error::new(
            field.span(),
            "a row key field cannot be Option",
        ));
    }
    if config.default {
        return Err(syn::Error::new(
            field.span(),
            "a row key field cannot use #[bigtable(default)]",
        ));
    }
    Ok(())
}

fn add_decoder_bound(
    generics: &mut Generics,
    target_type: &Type,
    config: &FieldConfig,
    crate_path: &TokenStream2,
) {
    if config.with.is_some() {
        return;
    }
    let bound: Path = if config.json {
        parse_quote!(#crate_path::FromJsonValue)
    } else {
        parse_quote!(#crate_path::FromCellValue)
    };
    add_bound(generics, target_type, &bound);
}

fn add_bound(generics: &mut Generics, target_type: &Type, bound: &Path) {
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#target_type: #bound));
}

fn option_inner(field_type: &Type) -> Option<&Type> {
    let Type::Path(type_path) = field_type else {
        return None;
    };
    if type_path.qself.is_some() {
        return None;
    }
    let segments = &type_path.path.segments;
    let is_option = match segments.len() {
        1 => segments[0].ident == "Option",
        3 => {
            (segments[0].ident == "std" || segments[0].ident == "core")
                && segments[1].ident == "option"
                && segments[2].ident == "Option"
        }
        _ => false,
    };
    if !is_option {
        return None;
    }
    let segment = segments.last()?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

fn client_crate() -> TokenStream2 {
    match crate_name("bigtable-client") {
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            quote!(::#ident)
        }
        Ok(FoundCrate::Itself) | Err(_) => quote!(::bigtable_client),
    }
}

#[derive(Default)]
struct StructConfig {
    family: Option<LitStr>,
}

impl StructConfig {
    fn parse(attributes: &[Attribute]) -> syn::Result<Self> {
        let mut config = Self::default();
        for attribute in attributes
            .iter()
            .filter(|attribute| attribute.path().is_ident("bigtable"))
        {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("family") {
                    set_once(
                        &mut config.family,
                        meta.value()?.parse()?,
                        meta.path.span(),
                        "family",
                    )
                } else {
                    Err(meta.error("unsupported struct-level bigtable attribute"))
                }
            })?;
        }
        Ok(config)
    }
}

#[derive(Default)]
struct FieldConfig {
    row_key: bool,
    family: Option<LitStr>,
    qualifier: Option<Qualifier>,
    json: bool,
    with: Option<Path>,
    default: bool,
}

impl FieldConfig {
    fn parse(field: &Field) -> syn::Result<Self> {
        let mut config = Self::default();
        for attribute in field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("bigtable"))
        {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("row_key") {
                    set_flag(&mut config.row_key, meta.path.span(), "row_key")
                } else if meta.path.is_ident("family") {
                    set_once(
                        &mut config.family,
                        meta.value()?.parse()?,
                        meta.path.span(),
                        "family",
                    )
                } else if meta.path.is_ident("qualifier") {
                    let expression: Expr = meta.value()?.parse()?;
                    let qualifier = Qualifier::parse(expression)?;
                    set_once(
                        &mut config.qualifier,
                        qualifier,
                        meta.path.span(),
                        "qualifier",
                    )
                } else if meta.path.is_ident("json") {
                    set_flag(&mut config.json, meta.path.span(), "json")
                } else if meta.path.is_ident("with") {
                    let path = meta
                        .value()?
                        .parse::<LitStr>()?
                        .parse_with(Path::parse_mod_style)?;
                    set_once(&mut config.with, path, meta.path.span(), "with")
                } else if meta.path.is_ident("default") {
                    set_flag(&mut config.default, meta.path.span(), "default")
                } else {
                    Err(meta.error("unsupported field-level bigtable attribute"))
                }
            })?;
        }
        if config.json && config.with.is_some() {
            return Err(syn::Error::new(
                field.span(),
                "a field cannot use both #[bigtable(json)] and #[bigtable(with = \"...\")]",
            ));
        }
        Ok(config)
    }
}

#[derive(Clone)]
struct Qualifier {
    bytes: Vec<u8>,
    span: Span,
}

impl Qualifier {
    fn new(bytes: &[u8], span: Span) -> Self {
        Self {
            bytes: bytes.to_vec(),
            span,
        }
    }

    fn parse(expression: Expr) -> syn::Result<Self> {
        let span = expression.span();
        match expression {
            Expr::Lit(ExprLit {
                lit: Lit::Str(value),
                ..
            }) => Ok(Self::new(value.value().as_bytes(), span)),
            Expr::Lit(ExprLit {
                lit: Lit::ByteStr(value),
                ..
            }) => Ok(Self::new(&value.value(), span)),
            _ => Err(syn::Error::new(
                span,
                "qualifier must be a string or byte string literal",
            )),
        }
    }

    fn literal(&self) -> LitByteStr {
        LitByteStr::new(&self.bytes, self.span)
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, span: Span, name: &str) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new(
            span,
            format!("duplicate bigtable attribute '{name}'"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn set_flag(slot: &mut bool, span: Span, name: &str) -> syn::Result<()> {
    if *slot {
        return Err(syn::Error::new(
            span,
            format!("duplicate bigtable attribute '{name}'"),
        ));
    }
    *slot = true;
    Ok(())
}
