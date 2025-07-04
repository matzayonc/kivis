use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn;

use crate::schema::{Schema, SchemaKey};

pub fn generate_record_impl(schema: &Schema, visibility: syn::Visibility) -> TokenStream {
    let name = &schema.name;
    let generics = &schema.generics;
    let other_attrs = &schema.attrs;
    let table_value = schema.table_value;

    // Split generics for use in impl
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let key_type = syn::Ident::new(&format!("{}Key", name), name.span());

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

    let incrementable = if only_id_type {
        quote! {
            impl kivis::Incrementable for #key_type {
                fn bounds() -> Option<(Self, Self)> {
                    Some((#key_type(0), #key_type(u64::MAX)))
                }
                fn next_id(&self) -> Option<Self> {
                    self.0.checked_add(1).map(|id| #key_type(id))
                }
            }
        }
    } else {
        quote! {
            impl kivis::Incrementable for #key_type {
                fn bounds() -> Option<(Self, Self)> {
                    None
                }
                fn next_id(&self) -> Option<Self> {
                    None
                }
            }
        }
    };

    let mut key_impl = quote! {
        #(#other_attrs)*
        #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        #visibility struct #key_type(#(pub #field_types),*);

        #incrementable
    };

    let mut index_values = Vec::new();

    for (i, index) in schema.indexes.iter().enumerate() {
        let field_name = &index.name;
        let field_type_pascal = field_name.to_string().to_case(Case::Pascal);
        let index_name =
            syn::Ident::new(&format!("{}{}Index", name, field_type_pascal), name.span());
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
            kivis::wrap_index::<#name, #index_name>(key, #index_name(self.#field_name.clone()))?
        });
    }

    let autoincrement = if only_id_type {
        quote! {
            None
        }
    } else {
        quote! {
            Some(#key_type(#(self.#field_names.clone()),*))
        }
    };

    let keyed_recordable = if only_id_type {
        quote! {}
    } else {
        quote! {
            impl #impl_generics kivis::KeyedRecordable for #name #ty_generics #where_clause {
                fn key(&self) -> Self::Key {
                    #key_type(#(self.#field_names.clone()),*)
                }
            }
        }
    };

    let main_impl = quote! {
        #key_impl

        impl #impl_generics kivis::RecordKey for #key_type #ty_generics #where_clause {
            type Record = #name;
        }

        impl #impl_generics kivis::Recordable for #name #ty_generics #where_clause {
            const SCOPE: u8 = #table_value;
            type Key = #key_type;

            fn maybe_key(&self) -> Option<Self::Key> {
                #autoincrement
            }

            fn index_keys(&self, key: Self::Key) -> Result<Vec<Vec<u8>>, kivis::SerializationError> {
                Ok(vec![#(#index_values,)*])
            }
        }

        #keyed_recordable
    };

    TokenStream::from(main_impl)
}
