use std::path::PathBuf;

use miette::Result;
use miette::{miette, Context, IntoDiagnostic};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;

enum LogFileOutputMode {
    None,
    Some {
        directory_path: PathBuf,
        log_file_name: String,
    },
}

/// Initialize the console and file logging.
/// If `log_output_file_path` is `Some`, the logs will be written to the specified file.
///
/// **IMPORTANT: Retain the returned `Option<WorkerGuard>` in scope, otherwise flushing to file will stop.**
///
/// ## Example
/// ```
/// use miette::Result;
/// use shared::logging::initialize_console_and_file_logging;
/// use std::path::PathBuf;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let _guard = initialize_console_and_file_logging(
///         Some(PathBuf::from("some/path")).as_ref()
///     )?;
///
///     // ...
///
///     // _guard will drop and the end of main.
/// }
/// ```
pub fn initialize_console_and_file_logging(
    log_output_file_path: Option<&PathBuf>,
) -> Result<Option<WorkerGuard>> {
    let log_output_mode = if let Some(output_path) = log_output_file_path {
        let directory = output_path
            .parent()
            .ok_or_else(|| miette!("Could not parse --logFilePath's parent directory path."))?;

        let file_name = output_path
            .file_name()
            .ok_or_else(|| miette!("Could not parse --logFilePath's file name."))?
            .to_string_lossy()
            .to_string();

        LogFileOutputMode::Some {
            directory_path: directory.to_path_buf(),
            log_file_name: file_name,
        }
    } else {
        LogFileOutputMode::None
    };


    let (log_file_subscriber, guard) = if let LogFileOutputMode::Some {
        directory_path,
        log_file_name,
    } = log_output_mode
    {
        let (non_blocking_appender, guard) = tracing_appender::non_blocking(
            tracing_appender::rolling::never(directory_path, log_file_name),
        );

        (Some(non_blocking_appender), Some(guard))
    } else {
        (None, None)
    };


    let console_subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    let file_layer = log_file_subscriber.map(|writer| {
        tracing_subscriber::fmt::Layer::default()
            .with_ansi(false)
            .with_writer(writer)
    });

    let master_subscriber = console_subscriber.with(file_layer);


    tracing::subscriber::set_global_default(master_subscriber)
        .into_diagnostic()
        .wrap_err_with(|| miette!("Failed to set up logging."))?;


    Ok(guard)
}
