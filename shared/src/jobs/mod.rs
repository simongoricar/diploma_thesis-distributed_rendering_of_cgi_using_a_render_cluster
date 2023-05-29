use std::fs::read_to_string;
use std::path::Path;

use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BlenderJob {
    pub job_name: String,

    pub job_description: Option<String>,

    pub project_file_path: String,

    /// Inclusive frame bound.
    pub frame_range_from: usize,

    /// Inclusive frame bound.
    pub frame_range_to: usize,

    pub wait_for_number_of_workers: usize,

    pub output_directory_path: String,

    pub output_file_name_format: String,
}

impl BlenderJob {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        if path.exists() && !path.is_file() {
            return Err(miette!("Path exists, but it is not a file!"));
        } else if !path.exists() {
            return Err(miette!("No such file!"));
        }

        let file_contents = read_to_string(path)
            .into_diagnostic()
            .wrap_err_with(|| miette!("Could not read job from file."))?;

        toml::from_str(&file_contents)
            .into_diagnostic()
            .wrap_err_with(|| {
                miette!("Could not parse TOML contents of job file.")
            })
    }
}


