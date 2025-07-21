use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;

use crate::schema::{Schema, SchemaKey};

pub fn generate_record_impl(schema: &Schema, visibility: syn::Visibility) -> TokenStream {
    let name = &schema.name;
    let generics = &schema.generics;
    let other_attrs = &schema.attrs;
    let table_value = schema.table_value;

    // Split generics for use in impl
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let key_type = syn::Ident::new(&format!("{name}Key"), name.span());

    let only_id_type = schema.keys.is_empty();
    let keys = if only_id_type {
        vec![SchemaKey {
            name: syn::Ident::new("id", name.span()),
            ty: syn::parse_quote!(u64),
        }]
    } else {
        schema.keys.clone()
    };

    // Generate key type and implementation based on number of key fields
    let field_types: Vec<_> = keys.iter().map(|k| &k.ty).collect();
    let field_names: Vec<_> = keys.iter().map(|k| &k.name).collect();

    let key_trait = if only_id_type {
        quote! {
            impl kivis::Incrementable for #key_type {
                fn bounds() -> (Self, Self) {
                    (#key_type(0), #key_type(u64::MAX))
                }
                fn next_id(&self) -> Option<Self> {
                    self.0.checked_add(1).map(|id| #key_type(id))
                }
            }
        }
    } else {
        quote! {
            impl #impl_generics kivis::HasKey for #name #ty_generics #where_clause {
                type Key = #key_type;
                fn key(c: &<Self::Key as kivis::DefineRecord>::Record) -> Self::Key {
                    #key_type(#(c.#field_names.clone()),*)
                }
            }
        }
    };

    let mut key_impl = quote! {
        #(#other_attrs)*
        #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        #visibility struct #key_type(#(pub #field_types),*);

        #key_trait
    };

    let mut index_values = Vec::new();

    for (i, index) in schema.indexes.iter().enumerate() {
        let field_name = &index.name;
        let field_type_pascal = field_name.to_string().to_case(Case::Pascal);
        let index_name = syn::Ident::new(&format!("{name}{field_type_pascal}Index"), name.span());
        let index_type = &index.ty;
        let index_impl = quote! {
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            #visibility struct #index_name(pub #index_type);

            impl kivis::Index for #index_name {
                type Key = #key_type;
                type Record = #name;
                const INDEX: u8 = #i as u8;
            }
        };
        key_impl.extend(index_impl);

        index_values.push(quote! {
            (#i as u8, &self.#field_name)
        });
    }

    let main_impl = quote! {
        #key_impl

        impl #impl_generics kivis::DefineRecord for #key_type #ty_generics #where_clause {
            type Record = #name;
        }

        impl #impl_generics kivis::Recordable for #name #ty_generics #where_clause {
            const SCOPE: u8 = #table_value;
            type Key = #key_type;

    fn index_keys(&self) -> Vec<(u8, &dyn kivis::KeyBytes)> {
        vec![#(#index_values,)*]
    }
        }
    };

    TokenStream::from(main_impl)
}
