use std::fs::create_dir_all;
use std::path::PathBuf;
use std::time::{Instant, SystemTime};

use log::{debug, info};
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::results::worker_trace::WorkerTraceBuilder;
use tokio::process::Command;

use crate::utilities::parse_with_base_directory_prefix;

pub struct BlenderJobRunner {
    blender_binary_path: PathBuf,

    base_directory_path: PathBuf,

    tracer: WorkerTraceBuilder,
}

impl BlenderJobRunner {
    pub fn new(
        blender_binary_path: PathBuf,
        base_directory_path: PathBuf,
        tracer: WorkerTraceBuilder,
    ) -> Result<Self> {
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
            tracer,
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
            debug!(
                "Before parsing: output_directory_path is {}",
                job.output_directory_path
            );

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
            debug!(
                "After parsing: output_directory_path is {}",
                output_path
            );

            output_path.push('/');
            output_path.push_str(&job.output_file_name_format);

            output_path
        };

        info!("Starting to render frame {}.", frame_index);

        let time_render_start = Instant::now();
        let systime_render_start = SystemTime::now();

        let blender_args = [
            &blender_file_path_str,
            "--background",
            "--render-output",
            &output_file_path_str,
            "--render-format",
            &job.output_file_format,
            "--render-frame",
            &frame_index.to_string(),
        ];

        debug!("Blender arguments: {:?}", blender_args);

        Command::new(&self.blender_binary_path)
            .args(blender_args)
            .output()
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed while executing Blender binary."))?;

        let systime_render_end = SystemTime::now();
        let render_duration = time_render_start.elapsed();

        info!(
            "Rendered frame {} in {:.4} seconds.",
            frame_index,
            render_duration.as_secs_f64()
        );

        self.tracer
            .trace_new_rendered_frame(
                frame_index,
                systime_render_start,
                systime_render_end,
            )
            .await;

        Ok(())
    }
}
