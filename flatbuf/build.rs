use std::env::consts::OS;
use std::path::Path;

fn main() {
    let flatc = match OS {
        "windows" => {
            flatc_rust::Flatc::from_path("./tools/flatc.exe")
        },
        "linux" => {
            flatc_rust::Flatc::from_path("./tools/flatc")
        }
        _ => {
            flatc_rust::Flatc::from_path("./tools/flatc")
        }
    };

    // testschema.fbs
    println!("cargo:rerun-if-changed=definitions/testschema.fbs");
    flatc.run(flatc_rust::Args {
        inputs: &[Path::new("definitions/testschema.fbs")],
        out_dir: Path::new("src/buffers"),
        ..Default::default()
    }).expect("flatc");
}
