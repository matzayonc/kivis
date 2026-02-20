use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;

use crate::schema::{FieldIdentifier, KeyStrategy, Schema, SchemaKey};

pub struct Generator(Schema);

impl Generator {
    pub fn new(schema: Schema) -> Self {
        Self(schema)
    }

    pub fn generate_record_impl(&self, visibility: &syn::Visibility) -> TokenStream {
        let name = &self.0.name;
        let key_type = syn::Ident::new(&format!("{name}Key"), name.span());

        // Generate keys from schema
        let keys = self.generate_keys();

        // Generate key implementation
        let mut key_impl = self.generate_key_impl(&key_type, &keys, visibility);

        // Generate index implementations
        let (index_impl, index_values) = self.generate_index_impls(&key_type, visibility);
        key_impl.extend(index_impl);

        let trait_impls = self.generate_main_impl(&key_type, &index_values);
        key_impl.extend(trait_impls);

        TokenStream::from(key_impl)
    }
    fn generate_keys(&self) -> Vec<SchemaKey> {
        match &self.0.key_strategy {
            KeyStrategy::Autoincrement => {
                vec![SchemaKey {
                    field_id: FieldIdentifier::Indexed(0),
                    ty: syn::parse_quote!(u64),
                }]
            }
            KeyStrategy::FieldKeys(keys) => keys.clone(),
            KeyStrategy::Derived(types) => {
                // Create multiple SchemaKey entries for derived keys
                types
                    .iter()
                    .enumerate()
                    .map(|(i, ty)| SchemaKey {
                        field_id: FieldIdentifier::Indexed(i),
                        ty: ty.clone(),
                    })
                    .collect()
            }
        }
    }

    fn generate_key_impl(
        &self,
        key_type: &syn::Ident,
        keys: &[SchemaKey],
        visibility: &syn::Visibility,
    ) -> proc_macro2::TokenStream {
        let other_attrs = &self.0.attrs;

        // Generate key type and implementation based on number of key fields
        let field_types: Vec<_> = keys.iter().map(|k| &k.ty).collect();

        let key_trait = self.generate_key_trait_impl(key_type, keys);

        quote! {
            #(#other_attrs)*
            #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            #visibility struct #key_type(#(pub #field_types),*);

            #key_trait
        }
    }

    fn generate_key_trait_impl(
        &self,
        key_type: &syn::Ident,
        keys: &[SchemaKey],
    ) -> proc_macro2::TokenStream {
        let name = &self.0.name;
        let (impl_generics, ty_generics, where_clause) = self.0.generics.split_for_impl();

        match &self.0.key_strategy {
            KeyStrategy::Autoincrement => {
                // Generate Incrementable for autoincrement keys
                quote! {
                    impl ::kivis::Incrementable for #key_type {
                        // const BOUNDS: (Self, Self) = (#key_type(0), #key_type(u64::MAX));
                        fn next_id(&self) -> ::core::option::Option<Self> {
                            self.0.checked_add(1).map(|id| #key_type(id))
                        }
                    }
                }
            }
            KeyStrategy::FieldKeys(_) => {
                // Generate DeriveKey for field-based keys
                // Generate field access based on field identifier type
                let field_accesses: Vec<proc_macro2::TokenStream> = keys
                    .iter()
                    .map(|key| match &key.field_id {
                        FieldIdentifier::Named(name) => quote! { c.#name.clone() },
                        FieldIdentifier::Indexed(idx) => {
                            let index = syn::Index::from(*idx);
                            quote! { c.#index.clone() }
                        }
                    })
                    .collect();

                quote! {
                    impl #impl_generics ::kivis::DeriveKey for #name #ty_generics #where_clause {
                        type Key = #key_type;
                        fn key(c: &<Self::Key as ::kivis::RecordKey>::Record) -> Self::Key {
                            #key_type(#(#field_accesses),*)
                        }
                    }
                }
            }
            KeyStrategy::Derived(_) => {
                // Don't generate any trait implementation for derived keys
                // The user must implement DeriveKey manually
                quote! {}
            }
        }
    }

    fn generate_index_impls(
        &self,
        key_type: &syn::Ident,
        visibility: &syn::Visibility,
    ) -> (proc_macro2::TokenStream, Vec<proc_macro2::TokenStream>) {
        let name = &self.0.name;
        let mut index_impl = proc_macro2::TokenStream::new();
        let mut index_values = Vec::new();

        for (i, index) in self.0.indexes.iter().enumerate() {
            // Generate a name for the index type based on field identifier
            let index_type_suffix = match &index.field_id {
                FieldIdentifier::Named(field_name) => field_name.to_string().to_case(Case::Pascal),
                FieldIdentifier::Indexed(idx) => format!("Field{idx}"),
            };
            let index_name =
                syn::Ident::new(&format!("{name}{index_type_suffix}Index"), name.span());
            let index_type = &index.ty;

            let current_index_impl = quote! {
                #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
                #visibility struct #index_name(pub #index_type);

                impl ::kivis::Index for #index_name {
                    type Key = #key_type;
                    type Record = #name;
                    const INDEX: u8 = #i as u8;
                }
            };
            index_impl.extend(current_index_impl);

            // Generate field access based on field identifier type
            let field_access = match &index.field_id {
                FieldIdentifier::Named(field_name) => {
                    quote! { &self.#field_name }
                }
                FieldIdentifier::Indexed(idx) => {
                    let index = syn::Index::from(*idx);
                    quote! { &self.#index }
                }
            };

            index_values.push(field_access);
        }

        (index_impl, index_values)
    }

    fn generate_main_impl(
        &self,
        key_type: &syn::Ident,
        index_values: &[proc_macro2::TokenStream],
    ) -> proc_macro2::TokenStream {
        let name = &self.0.name;
        let (impl_generics, ty_generics, where_clause) = self.0.generics.split_for_impl();

        let Ok(index_count) = u8::try_from(index_values.len()) else {
            return quote! {
                compile_error!("Too many indexes: maximum of 256 indexes allowed per record");
            };
        };
        let indices = 0..index_count;

        quote! {
            impl #impl_generics ::kivis::RecordKey for #key_type #ty_generics #where_clause {
                type Record = #name;
            }

            impl #impl_generics ::kivis::DatabaseEntry for #name #ty_generics #where_clause {
                type Key = #key_type;
                const INDEX_COUNT_HINT: u8 = #index_count as u8;

                fn index_key<KU: ::kivis::Unifier>(
                    &self,
                    buffer: &mut KU::D,
                    discriminator: u8,
                    serializer: &KU,
                ) -> core::result::Result<(), kivis::BufferOverflowOr<KU::SerError>> {
                    match discriminator {
                        #(
                            #indices => {
                                serializer.serialize_ref(buffer, #index_values)?;
                            }
                        )*
                        _ => {}
                    }
                    Ok(())
                }
            }
        }
    }
}
