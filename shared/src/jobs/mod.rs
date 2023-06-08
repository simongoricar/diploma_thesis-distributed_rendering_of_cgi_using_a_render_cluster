use std::fs::read_to_string;
use std::path::Path;

use miette::{miette, Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Debug)]
pub struct DynamicStrategyOptions {
    /// This is essentially the maximum queue size for each worker.
    /// When the amount of queued frames dips below this amount,
    /// we'll try to queue more.
    pub target_queue_size: usize,

    /// When all the frames have already been queued, we attempt to "steal"
    /// frames from other workers. This specifies the minimum queue size
    /// to attempt to steal some queued frame from a worker.
    pub min_queue_size_to_steal: usize,

    /// When stealing a frame, we need to prevent the frame from constantly
    /// being reassigned to various workers, wasting network traffic and performance.
    /// This specifies how long a frame must stay queued on a worker for until
    /// it can be reassigned (i.e. stolen) to a different worker.
    pub min_seconds_before_resteal_to_elsewhere: usize,

    /// This should be larger than `min_seconds_before_resteal_to_elsewhere`,
    /// and it essentially specifies how long until we can reassign a frame back to
    /// the worker it was stolen from. Unless there are only two workers
    /// this should happen very rarely, if at all.
    pub min_seconds_before_resteal_to_original_worker: usize,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Debug)]
#[serde(tag = "strategy_type")]
pub enum DistributionStrategy {
    #[serde(rename = "naive-fine")]
    NaiveFine,

    #[serde(rename = "naive-coarse")]
    NaiveCoarse { target_queue_size: usize },

    #[serde(rename = "dynamic")]
    Dynamic(DynamicStrategyOptions),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct BlenderJob {
    /// Name of the job.
    pub job_name: String,

    /// Short description of the job, if provided.
    pub job_description: Option<String>,

    /// Path to the `.blend` project file (must be accessible to workers,
    /// but can use the %BASE% placeholder).
    pub project_file_path: String,

    /// Frame start bound (inclusive).
    pub frame_range_from: usize,

    /// Frame end bound (inclusive).
    pub frame_range_to: usize,

    /// How many workers must connect before starting job.
    /// **This is essentially the number of workers you're running this job with.**
    pub wait_for_number_of_workers: usize,

    pub frame_distribution_strategy: DistributionStrategy,

    /// Frame output directory path (can use %BASE% placeholder).
    pub output_directory_path: String,

    /// Format of the output file names (e.g. "rendered-#####").
    pub output_file_name_format: String,

    /// File format (`PNG`, ...), see the Blender CLI documentation.
    pub output_file_format: String,
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
            .wrap_err_with(|| miette!("Could not parse TOML contents of job file."))
    }
}
