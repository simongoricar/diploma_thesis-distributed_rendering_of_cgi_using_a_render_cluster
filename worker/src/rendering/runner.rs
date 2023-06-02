use std::fs::create_dir_all;
use std::path::PathBuf;
use std::time::Instant;

use log::info;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use tokio::process::Command;

use crate::utilities::parse_with_base_directory_prefix;

pub struct BlenderJobRunner {
    blender_binary_path: PathBuf,
    base_directory_path: PathBuf,
}

impl BlenderJobRunner {
    pub fn new(blender_binary_path: PathBuf, base_directory_path: PathBuf) -> Result<Self> {
        if !blender_binary_path.is_file() {
            return Err(miette!("Provided Blender path is not a file."));
        }

        if !base_directory_path.is_dir() {
            return Err(miette!(
                "Provided base directory path is not a directory."
            ));
        }

        Ok(Self {
            blender_binary_path,
            base_directory_path,
        })
    }

    pub async fn render_frame(&self, job: BlenderJob, frame_index: usize) -> Result<()> {
        /*
         * Parse path to .blend project file
         */
        let blender_file_path = parse_with_base_directory_prefix(
            &job.project_file_path,
            Some(&self.base_directory_path),
        )?;

        // Ensure blender project file exists.
        if !blender_file_path.is_file() {
            return Err(miette!(
                "Invalid blender project file path: file doesn't exist: {:?}",
                blender_file_path
            ));
        }

        let blender_file_path_str = blender_file_path.to_string_lossy().to_string();

        /*
         * Parse output file
         */
        let output_file_path_str = {
            let output_directory = parse_with_base_directory_prefix(
                &job.output_directory_path,
                Some(&self.base_directory_path),
            )?;

            if !output_directory.is_dir() {
                create_dir_all(&output_directory)
                    .into_diagnostic()
                    .wrap_err_with(|| miette!("Could not create missing directories."))?;
            }

            let mut output_path = output_directory.to_string_lossy().to_string();
            output_path.push_str(&job.output_file_name_format);

            output_path
        };

        info!("Starting to render frame {}.", frame_index);

        let time_render_start = Instant::now();

        Command::new(&self.blender_binary_path)
            .args([
                &blender_file_path_str,
                "--background",
                "--render-output",
                &output_file_path_str,
                "--render-format",
                "PNG",
                "--render-frame",
                &frame_index.to_string(),
            ])
            .output()
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed while executing Blender binary."))?;

        let render_duration = time_render_start.elapsed();

        info!(
            "Rendered frame {} in {:.4} seconds.",
            frame_index,
            render_duration.as_secs_f64()
        );

        Ok(())
    }
}