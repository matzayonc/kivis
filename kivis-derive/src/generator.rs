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
    let key_impl = quote! {
            #(#other_attrs)*
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
            pub struct #key_type(#(pub #field_types),*);

            impl #impl_generics kivis::Recordable for #name #ty_generics #where_clause {
                const SCOPE: u8 = #table_value;
                type Key = #key_type;

                fn key(&self) -> Self::Key {
                    #key_type(#(self.#field_names.clone()),*)
                }
            }
    };

    TokenStream::from(key_impl)
}
