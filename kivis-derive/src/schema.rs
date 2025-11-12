use proc_macro::TokenStream;
use syn::{Data, DeriveInput, Error, Fields, Ident, Type};

#[derive(Clone)]
pub enum FieldIdentifier {
    Named(Ident),
    Indexed(usize),
}

#[derive(Clone)]
pub struct SchemaKey {
    pub field_id: FieldIdentifier,
    pub ty: Type,
}

#[derive(Clone)]
pub enum KeyStrategy {
    /// Autoincrement key (no explicit keys, no derived_key attribute)
    Autoincrement,
    /// Explicit field keys (fields marked with #[key])
    FieldKeys(Vec<SchemaKey>),
    /// Derived keys (struct has #[derived_key(...)] attribute)
    Derived(Vec<Type>),
}

pub struct Schema {
    pub name: Ident,
    pub generics: syn::Generics,
    pub attrs: Vec<syn::Attribute>,
    pub key_strategy: KeyStrategy,
    pub indexes: Vec<SchemaKey>,
}

impl Schema {
    pub fn from_derive_input(input: DeriveInput) -> Result<Self, TokenStream> {
        let name = input.ident;
        let generics = input.generics;
        let attrs = input
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("derived_key"))
            .cloned()
            .collect::<Vec<_>>();

        // Ensure it's a struct
        let fields = match input.data {
            Data::Struct(ref data_struct) => &data_struct.fields,
            _ => {
                return Err(Error::new_spanned(
                    &name,
                    "kivis::Record can only be derived for structs",
                )
                .to_compile_error()
                .into());
            }
        };

        // Handle both named and unnamed (tuple) fields
        let field_list = match fields {
            Fields::Named(fields) => fields.named.iter().collect::<Vec<_>>(),
            Fields::Unnamed(fields) => fields.unnamed.iter().collect::<Vec<_>>(),
            Fields::Unit => {
                return Err(Error::new_spanned(
                    &name,
                    "Unit structs cannot derive kivis::Record as they have no fields",
                )
                .to_compile_error()
                .into());
            }
        };

        if field_list.is_empty() {
            return Err(Error::new_spanned(
                &name,
                "Struct must have at least one field to derive kivis::Record",
            )
            .to_compile_error()
            .into());
        }

        // Find fields marked with #[key] attribute
        let mut key_fields = Vec::new();
        for (index, field) in field_list.iter().enumerate() {
            let has_key_attr = field.attrs.iter().any(|attr| attr.path().is_ident("key"));
            if has_key_attr {
                let field_id = if let Some(ident) = &field.ident {
                    FieldIdentifier::Named(ident.clone())
                } else {
                    FieldIdentifier::Indexed(index)
                };
                key_fields.push(SchemaKey {
                    field_id,
                    ty: field.ty.clone(),
                });
            }
        }

        // Look for the derived_key attribute
        let derived_key_types = input
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("derived_key"))
            .map(|attr| {
                // Parse #[derived_key(Type1, Type2, ...)]
                attr.parse_args_with(
                    syn::punctuated::Punctuated::<Type, syn::Token![,]>::parse_terminated,
                )
                .map(|types| types.into_iter().collect::<Vec<_>>())
                .unwrap_or_default()
            })
            .unwrap_or_default();

        // Determine key strategy and validate that only one is used
        let key_strategy = match (key_fields.is_empty(), derived_key_types.is_empty()) {
            (true, true) => KeyStrategy::Autoincrement,
            (false, true) => KeyStrategy::FieldKeys(key_fields),
            (true, false) => KeyStrategy::Derived(derived_key_types),
            (false, false) => {
                return Err(Error::new_spanned(
                    &name,
                    "Cannot use both #[key] field attributes and #[derived_key] attribute on the same struct. Choose one key strategy.",
                )
                .to_compile_error()
                .into());
            }
        };

        let index_fields = field_list
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if field.attrs.iter().any(|attr| attr.path().is_ident("index")) {
                    let field_id = if let Some(ident) = &field.ident {
                        FieldIdentifier::Named(ident.clone())
                    } else {
                        FieldIdentifier::Indexed(index)
                    };
                    Some(SchemaKey {
                        field_id,
                        ty: field.ty.clone(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        Ok(Schema {
            name,
            generics,
            attrs,
            key_strategy,
            indexes: index_fields,
        })
    }
}
