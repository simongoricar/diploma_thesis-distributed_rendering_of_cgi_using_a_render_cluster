mod utilities;

use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::Stdio;

use chrono::Utc;
use miette::{miette, Context, IntoDiagnostic, Result};
use shared::jobs::BlenderJob;
use shared::results::worker_trace::WorkerTraceBuilder;
use tokio::process::Command;
use tracing::{debug, info};

use crate::rendering::runner::utilities::extract_blender_render_information;
use crate::utilities::parse_with_base_directory_prefix;


pub struct BlenderJobRunner {
    blender_binary_path: PathBuf,

    blender_prepend_arguments: Vec<String>,

    blender_append_arguments: Vec<String>,

    base_directory_path: PathBuf,

    tracer: WorkerTraceBuilder,
}

impl BlenderJobRunner {
    pub fn new(
        blender_binary: PathBuf,
        blender_prepend_arguments: Option<String>,
        blender_append_arguments: Option<String>,
        base_directory_path: PathBuf,
        tracer: WorkerTraceBuilder,
    ) -> Result<Self> {
        if !blender_binary.is_file() {
            return Err(miette!("Provided Blender path is not a file."));
        }

        if !base_directory_path.is_dir() {
            return Err(miette!(
                "Provided base directory path is not a directory."
            ));
        }

        let parsed_prepend_arguments: Vec<String> = if let Some(prepend) = blender_prepend_arguments
        {
            shlex::split(&prepend).ok_or_else(|| miette!("Failed to parse prepend arguments."))?
        } else {
            Vec::new()
        };

        let parsed_append_arguments: Vec<String> = if let Some(append) = blender_append_arguments {
            shlex::split(&append).ok_or_else(|| miette!("Failed to parse append arguments."))?
        } else {
            Vec::new()
        };

        debug!(
            "Parsed prepend arguments: {:?}",
            parsed_prepend_arguments
        );

        Ok(Self {
            blender_binary_path: blender_binary,
            blender_prepend_arguments: parsed_prepend_arguments,
            blender_append_arguments: parsed_append_arguments,
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
         * Parse path to .py script file
         */
        let render_script_path = parse_with_base_directory_prefix(
            &job.render_script_path,
            Some(&self.base_directory_path),
        )?;

        if !render_script_path.is_file() {
            return Err(miette!(
                "Invalid render script: file doesn't exist: {:?}",
                blender_file_path
            ));
        }

        let render_script_path_str = render_script_path.to_string_lossy().to_string();

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

        let mut blender_args = self.blender_prepend_arguments.clone();
        blender_args.extend(
            [
                &blender_file_path_str,
                "--background",
                "--python",
                &render_script_path_str,
                "--",
                "--render-output",
                &output_file_path_str,
                "--render-format",
                &job.output_file_format,
                "--render-frame",
                &frame_index.to_string(),
            ]
            .into_iter()
            .map(String::from),
        );
        blender_args.extend(self.blender_append_arguments.iter().cloned());

        debug!("Blender arguments: {:?}", blender_args);

        let process_start_time = Utc::now();

        let output = Command::new(&self.blender_binary_path)
            .args(blender_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await
            .into_diagnostic()
            .wrap_err_with(|| miette!("Failed while executing Blender binary."))?;

        let process_stop_time = Utc::now();

        /*
         * Parse output of render script and file writing time.
         */
        let stdout_output = String::from_utf8_lossy(&output.stdout);
        let blender_statistics = extract_blender_render_information(stdout_output.as_ref())
            .wrap_err_with(|| {
                format!(
                    "Failed to extract render information:\n{}.",
                    stdout_output.as_ref()
                )
            })?;
        let blender_statistics =
            blender_statistics.with_process_information(process_start_time, process_stop_time);

        info!(
            "Rendered frame {} in {:.4} seconds.",
            frame_index,
            blender_statistics.total_execution_time()?.as_secs_f64()
        );

        self.tracer
            .trace_new_rendered_frame(frame_index, blender_statistics)
            .await;

        Ok(())
    }
}
