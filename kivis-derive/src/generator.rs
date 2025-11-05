use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;

use crate::schema::{Schema, SchemaKey};

pub struct Generator(Schema);

impl Generator {
    pub fn new(schema: Schema) -> Self {
        Self(schema)
    }

    pub fn generate_record_impl(&self, visibility: syn::Visibility) -> TokenStream {
        let name = &self.0.name;
        let key_type = syn::Ident::new(&format!("{name}Key"), name.span());

        // Generate keys from schema
        let keys = self.generate_keys();

        // Generate key implementation
        let mut key_impl = self.generate_key_impl(&key_type, &keys, &visibility);

        // Generate index implementations
        let (index_impl, index_values) = self.generate_index_impls(&key_type, &visibility);
        key_impl.extend(index_impl);

        let trait_impls = self.generate_main_impl(&key_type, &index_values);
        key_impl.extend(trait_impls);

        TokenStream::from(key_impl)
    }

    fn generate_keys(&self) -> Vec<SchemaKey> {
        if self.0.keys.is_empty() {
            vec![SchemaKey {
                name: syn::Ident::new("id", self.0.name.span()),
                ty: syn::parse_quote!(u64),
            }]
        } else {
            self.0.keys.clone()
        }
    }

    fn generate_key_impl(
        &self,
        key_type: &syn::Ident,
        keys: &[SchemaKey],
        visibility: &syn::Visibility,
    ) -> proc_macro2::TokenStream {
        let other_attrs = &self.0.attrs;
        let only_id_type = self.0.keys.is_empty();

        // Generate key type and implementation based on number of key fields
        let field_types: Vec<_> = keys.iter().map(|k| &k.ty).collect();
        let field_names: Vec<_> = keys.iter().map(|k| &k.name).collect();

        let key_trait = self.generate_key_trait_impl(only_id_type, key_type, &field_names);

        quote! {
            #(#other_attrs)*
            #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            #visibility struct #key_type(#(pub #field_types),*);

            #key_trait
        }
    }

    fn generate_key_trait_impl(
        &self,
        only_id_type: bool,
        key_type: &syn::Ident,
        field_names: &[&syn::Ident],
    ) -> proc_macro2::TokenStream {
        let name = &self.0.name;
        let (impl_generics, ty_generics, where_clause) = self.0.generics.split_for_impl();

        if only_id_type {
            quote! {
                impl kivis::Incrementable for #key_type {
                    // const BOUNDS: (Self, Self) = (#key_type(0), #key_type(u64::MAX));
                    fn next_id(&self) -> Option<Self> {
                        self.0.checked_add(1).map(|id| #key_type(id))
                    }
                }
            }
        } else {
            quote! {
                impl #impl_generics kivis::DeriveKey for #name #ty_generics #where_clause {
                    type Key = #key_type;
                    fn key(c: &<Self::Key as kivis::RecordKey>::Record) -> Self::Key {
                        #key_type(#(c.#field_names.clone()),*)
                    }
                }
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
            let field_name = &index.name;
            let field_type_pascal = field_name.to_string().to_case(Case::Pascal);
            let index_name =
                syn::Ident::new(&format!("{name}{field_type_pascal}Index"), name.span());
            let index_type = &index.ty;
            let current_index_impl = quote! {
                #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
                #visibility struct #index_name(pub #index_type);

                impl kivis::Index for #index_name {
                    type Key = #key_type;
                    type Record = #name;
                    const INDEX: u8 = #i as u8;
                }
            };
            index_impl.extend(current_index_impl);

            index_values.push(quote! {
                (#i as u8, &self.#field_name)
            });
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

        quote! {
            impl #impl_generics kivis::RecordKey for #key_type #ty_generics #where_clause {
                type Record = #name;
            }

            impl #impl_generics kivis::DatabaseEntry for #name #ty_generics #where_clause {
                type Key = #key_type;

                fn index_keys(&self) -> Vec<(u8, &dyn kivis::KeyBytes)> {
                    vec![#(#index_values,)*]
                }
            }
        }
    }
}
