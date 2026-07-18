use quote::quote;

pub fn fixify_version(
  fix_version: &str,
  version: &crate::FixVersion,
) -> proc_macro2::TokenStream {
  let version_ident = syn::Ident::new(
    fix_version.replace(".", "_").as_str(),
    proc_macro2::Span::call_site(),
  );

  let mut fields = vec![];
  for (tag, field) in &version.fields {
    let field_ident =
      syn::Ident::new(field.name.as_str(), proc_macro2::Span::call_site());
    fields.push(quote! {
      pub const #field_ident : u32 = #tag;
    });
  }

  quote! {
    pub mod #version_ident {
      pub mod Fields {
        #( #fields )*
      }
    }
  }
}
