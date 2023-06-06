use std::path::{Path, PathBuf};

use miette::{miette, Context, IntoDiagnostic, Result};

pub fn parse_with_base_directory_prefix(path: &str, base: Option<&PathBuf>) -> Result<PathBuf> {
    if path.starts_with("%BASE%") {
        if base.is_none() {
            return Err(miette!("Missing base!"));
        }

        let prefixless_path = path.strip_prefix("%BASE%").expect("BUG: Missing prefix.");
        let prefixless_path = prefixless_path.strip_prefix('/').unwrap_or(prefixless_path);
        let prefixless_path = prefixless_path
            .strip_prefix('\\')
            .unwrap_or(prefixless_path);

        Ok(base.unwrap().join(prefixless_path))
    } else {
        Ok(PathBuf::from(path))
    }
}

pub fn parse_with_tilde_support<P: AsRef<Path>>(path: P) -> Result<PathBuf> {
    let path = path.as_ref();

    if path.starts_with("~") {
        let home_var = std::env::var("HOME")
            .into_diagnostic()
            .wrap_err_with(|| miette!("Path contained ~, but no HOME environment variable!"))?;

        let without_tilde = path.strip_prefix("~").unwrap();

        Ok(PathBuf::from(home_var).join(without_tilde))
    } else {
        Ok(PathBuf::from(path))
    }
}
