use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, parse_macro_input};

#[proc_macro_derive(Record, attributes(table))]
pub fn derive_key(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let other_attrs = &input.attrs;

    // Look for the table attribute to get the record type
    let table_value = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("table"))
        .and_then(|attr| attr.parse_args::<syn::LitInt>().ok())
        .map(|lit| lit.base10_parse::<u8>().unwrap_or(1))
        .unwrap_or(1);

    // Ensure it's a struct
    let fields = match input.data {
        Data::Struct(ref data_struct) => &data_struct.fields,
        _ => {
            return Error::new_spanned(name, "kivis::Record can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    // Get the first field
    let (first_field, first_field_type) = match fields {
        Fields::Named(fields) => {
            if let Some(field) = fields.named.iter().next() {
                if let Some(ident) = &field.ident {
                    (ident, field.ty.clone())
                } else {
                    return Error::new_spanned(
                        name,
                        "Couldn't find field identifier for first field",
                    )
                    .to_compile_error()
                    .into();
                }
            } else {
                return Error::new_spanned(
                    name,
                    "Struct must have at least one field to derive kivis::Record",
                )
                .to_compile_error()
                .into();
            }
        }
        Fields::Unnamed(fields) => {
            if fields.unnamed.iter().next().is_some() {
                return Error::new_spanned(
                    name,
                    "kivis::Record doesn't support tuple structs, use named fields",
                )
                .to_compile_error()
                .into();
            } else {
                return Error::new_spanned(
                    name,
                    "Struct must have at least one field to derive kivis::Record",
                )
                .to_compile_error()
                .into();
            }
        }
        Fields::Unit => {
            return Error::new_spanned(
                name,
                "Unit structs cannot derive kivis::Record as they have no fields",
            )
            .to_compile_error()
            .into();
        }
    };

    // Split generics for use in impl
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let key_type = syn::Ident::new(&format!("{}Key", name), name.span());

    // Generate the key() method implementation
    let key_impl = quote! {
        #(#other_attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub struct #key_type(pub #first_field_type);

        impl #impl_generics kivis::Recordable for #name #ty_generics #where_clause {
            const SCOPE: u8 = #table_value;
            type Key = #key_type;

            fn key(&self) -> Self::Key {
                #key_type(self.#first_field.clone())
            }
        }
    };

    // Return the generated impl
    TokenStream::from(key_impl)
}
