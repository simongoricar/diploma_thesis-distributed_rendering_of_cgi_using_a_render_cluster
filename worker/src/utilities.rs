use std::path::PathBuf;

use miette::{miette, Result};

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
