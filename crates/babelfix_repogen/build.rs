use babelfix_repo as repository;
use quote::quote;

pub fn generate_repo_static() -> Result<String, Box<dyn std::error::Error>> {
  let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or("./".into());

  let mut repo = repository::FixRepository::default();
  for f in &[
    "OrchestraFIX42.xml",
    "OrchestraFIX44.xml",
    "OrchestraFIXLatest.xml",
  ] {
    let p = std::path::Path::new(&manifest_dir)
      .join("../../third-party/fix_orchestra")
      .join(f);
    println!("cargo:rerun-if-changed={}", p.display());
    repo = repo.merge(repository::load_orchestration(p)?);
  }

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
