use std::env::consts::OS;
use std::path::Path;

use flatc_rust::Flatc;

const DEFINITIONS_DIRECTORY_PATH: &str = "definitions";
const OUTPUT_DIRECTORY_PATH: &str = "src/compiled_definitions";

fn compile_schema(flatc_compiler: &Flatc, schema_name: &str) {
    println!("cargo:rerun-if-changed=definitions/{schema_name}");

    flatc_compiler
        .run(flatc_rust::Args {
            inputs: &[&Path::new(DEFINITIONS_DIRECTORY_PATH).join(schema_name)],
            out_dir: Path::new(OUTPUT_DIRECTORY_PATH),
            ..Default::default()
        })
        .expect("flatc");
}

fn main() {
    let flatc = match OS {
        "windows" => Flatc::from_path("./tools/flatc.exe"),
        "linux" => Flatc::from_path("./tools/flatc"),
        _ => Flatc::from_path("./tools/flatc"),
    };


    // Compile all schemas (add any new ones here).
    compile_schema(&flatc, "handshake_request.fbs");
    compile_schema(&flatc, "handshake_response.fbs");
    compile_schema(&flatc, "handshake_acknowledgement.fbs");
}
