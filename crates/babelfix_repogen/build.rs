use babelfix_repo as repository;
use quote::quote;

pub fn generate_repo_static() -> Result<String, Box<dyn std::error::Error>> {
  // Source the FIX Orchestra data from the babelfix-repo crate, which embeds
  // it via include_dir!. This keeps the third-party (Apache-2.0) data owned by
  // a single crate and lets the generator work without any on-disk paths.
  let repo = repository::orchestrate()?;

  let versions: Vec<_> = repo
    .versions
    .iter()
    .map(|v| repository::fixify::fixify_version(v.0, v.1))
    .collect();

  Ok(
    quote! {
      #( #versions )*
    }
    .to_string(),
  )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let out_dir = std::env::var("OUT_DIR")?;
  let out_file = std::path::PathBuf::from(out_dir).join("repo_static.rs");

  let generated = generate_repo_static()?;
  std::fs::write(out_file, generated)?;
  Ok(())
}
