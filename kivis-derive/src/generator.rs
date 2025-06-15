use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::quote;
use syn;

use crate::schema::Schema;

pub fn generate_record_impl(schema: &Schema) -> TokenStream {
    let name = &schema.name;
    let generics = &schema.generics;
    let other_attrs = &schema.attrs;
    let table_value = schema.table_value;
    let keys = &schema.keys;

    // Split generics for use in impl
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let key_type = syn::Ident::new(&format!("{}Key", name), name.span());

    // Generate key type and implementation based on number of key fields
    let field_types: Vec<_> = keys.iter().map(|k| &k.ty).collect();
    let field_names: Vec<_> = keys.iter().map(|k| &k.name).collect();
    let mut key_impl = quote! {
            #(#other_attrs)*
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            pub struct #key_type(#(pub #field_types),*);

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
            pub struct #index_name(pub #index_type);

            impl kivis::Indexed for #index_name {
                type Key = #key_type;
                type Record = #name;
                const INDEX: u8 = #i as u8;
            }
        };
        key_impl.extend(index_impl);

        index_values.push( quote! {
            kivis::wrap_index::<#name, #index_name>(self.key(), #index_name(self.#field_name.clone()))?
        });
    }

    let main_impl = quote! {
        #key_impl

        impl #impl_generics kivis::Recordable for #name #ty_generics #where_clause {
            const SCOPE: u8 = #table_value;
            type Key = #key_type;

            fn key(&self) -> Self::Key {
                #key_type(#(self.#field_names.clone()),*)
            }

            fn index_keys(&self) -> Result<Vec<Vec<u8>>, kivis::SerializationError> {
                Ok(vec![#(#index_values,)*])
            }
        }
    };

    TokenStream::from(main_impl)
}
