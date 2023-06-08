use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
pub struct CLIArgs {
    #[arg(long = "masterServerHost", help = "Host of the master server.")]
    pub master_server_host: String,

    #[arg(long = "masterServerPort", help = "Port of the master server.")]
    pub master_server_port: usize,

    #[arg(
        long = "baseDirectory",
        help = "Sets the value of the %BASE% placeholder used in many job files."
    )]
    pub base_directory_path: PathBuf,

    #[arg(
        short = 'b',
        long = "blenderBinary",
        help = "Blender binary to use for rendering."
    )]
    pub blender_binary: PathBuf,

    #[arg(
        short = 'p',
        long = "blenderPrependArguments",
        help = "Additional arguments to append after the binary and before the actual Blender arguments when rendering."
    )]
    pub blender_prepend_arguments: Option<String>,

    #[arg(
        short = 'a',
        long = "blenderAppendArguments",
        help = "Additional arguments to append after the all other arguments when rendering."
    )]
    pub blender_append_arguments: Option<String>,
}
