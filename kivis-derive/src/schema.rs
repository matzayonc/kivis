use proc_macro::TokenStream;
use syn::{Data, DeriveInput, Error, Fields, Ident, Type};

#[derive(Clone)]
pub struct SchemaKey {
    pub name: Ident,
    pub ty: Type,
}

pub struct Schema {
    pub name: Ident,
    pub generics: syn::Generics,
    pub attrs: Vec<syn::Attribute>,
    pub table_value: Option<u8>,
    pub keys: Vec<SchemaKey>,
    pub indexes: Vec<SchemaKey>,
}

impl Schema {
    pub fn from_derive_input(input: DeriveInput) -> Result<Self, TokenStream> {
        let name = input.ident;
        let generics = input.generics;
        let attrs = input
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("external"))
            .cloned()
            .collect::<Vec<_>>();

        // Look for the table attribute to get the record type
        let table_value = if let Some(table_value) = input
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("external"))
        {
            let Some(table_value) = table_value.parse_args::<syn::LitInt>().ok() else {
                return Err(Error::new_spanned(
                    &name,
                    "Invalid #[external(value)] attribute. Expected an integer literal.",
                )
                .to_compile_error()
                .into());
            };

            let Ok(table_value) = table_value.base10_parse::<u8>() else {
                return Err(Error::new_spanned(
                &name,
                "Invalid #[external(value)] attribute could not be parsed. Expected an integer literal.",
            )
            .to_compile_error()
            .into());
            };

            Some(table_value)
        } else {
            None
        };

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

        // Get named fields
        let named_fields = match fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) => {
                return Err(Error::new_spanned(
                    &name,
                    "kivis::Record doesn't support tuple structs, use named fields",
                )
                .to_compile_error()
                .into());
            }
            Fields::Unit => {
                return Err(Error::new_spanned(
                    &name,
                    "Unit structs cannot derive kivis::Record as they have no fields",
                )
                .to_compile_error()
                .into());
            }
        };

        if named_fields.is_empty() {
            return Err(Error::new_spanned(
                &name,
                "Struct must have at least one field to derive kivis::Record",
            )
            .to_compile_error()
            .into());
        }

        // Find fields marked with #[key] attribute
        let mut key_fields = Vec::new();
        for field in named_fields {
            let has_key_attr = field.attrs.iter().any(|attr| attr.path().is_ident("key"));
            if has_key_attr {
                if let Some(ident) = &field.ident {
                    key_fields.push(SchemaKey {
                        name: ident.clone(),
                        ty: field.ty.clone(),
                    });
                } else {
                    return Err(Error::new_spanned(&name, "Key field must have a name")
                        .to_compile_error()
                        .into());
                }
            }
        }

        let index_fields = named_fields
            .iter()
            .filter_map(|field| {
                if field.attrs.iter().any(|attr| attr.path().is_ident("index")) {
                    field.ident.as_ref().map(|ident| SchemaKey {
                        name: ident.clone(),
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
            table_value,
            keys: key_fields,
            indexes: index_fields,
        })
    }
}
