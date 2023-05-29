use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
pub struct CLIArgs {
    #[arg(
        short = 'm',
        long = "masterServer",
        help = "Host and port of the master server.",
        default_value = "127.0.0.1:9901"
    )]
    pub master_server_host_and_port: String,

    #[arg(
        long = "baseDirectory",
        help = "Sets the value of the %BASE% placeholder used in many job files."
    )]
    pub base_directory_path: PathBuf,

    #[arg(
        short = 'b',
        long = "blenderBinary",
        help = "Path to the Blender binary to use for rendering."
    )]
    pub blender_binary_path: PathBuf,
}
