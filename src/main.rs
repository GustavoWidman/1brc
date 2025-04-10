use anyhow::Context;
use one_billion_row_challenge::{IN_FILE_PATH, NUM_WORKERS, OUT_FILE_PATH, STATIONS_IN_DATASET};
use std::io::Write;

mod file;
mod hashmap;
mod measurement;

fn main() -> anyhow::Result<()> {
    if std::fs::metadata("{OUT_FILE_PATH}").is_ok() {
        std::fs::remove_file("{OUT_FILE_PATH}").context("Failed to remove {OUT_FILE_PATH}")?;
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_WORKERS)
        .build_global()
        .unwrap();

    let num_workers = rayon::current_num_threads();
    println!("Number of workers: {}", num_workers);
    assert_eq!(num_workers, NUM_WORKERS);

    let start = std::time::Instant::now();

    let file = file::File::open(IN_FILE_PATH).context(format!("Failed to open {IN_FILE_PATH}"))?;

    let measurements = file.parse();

    println!("Calculations took {:?}", start.elapsed());

    // print the final measurements
    let mut output = std::fs::File::create(OUT_FILE_PATH)
        .context(format!("Failed to create {OUT_FILE_PATH}"))?;

    write!(output, "{{").context(format!("Failed to write to {OUT_FILE_PATH}"))?;
    for (i, (city, measurement)) in measurements.iter().enumerate() {
        write!(
            output,
            "{}={:.1}/{:.1}/{:.1}",
            city, measurement.min, measurement.avg, measurement.max
        )
        .context(format!("Failed to write to {OUT_FILE_PATH}"))?;

        if i != measurements.len() - 1 {
            write!(output, ", ").context(format!("Failed to write to {OUT_FILE_PATH}"))?;
        }
    }
    write!(output, "}}\n").context(format!("Failed to write to {OUT_FILE_PATH}"))?;

    println!("Full took {:?}", start.elapsed());

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use crate::file;
    use std::{fs, io::Write, path::PathBuf};

    #[test]
    fn test_measurement_data() {
        let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
        let files = fs::read_dir(test_dir).unwrap();

        for file in files {
            let test_file_name = file.unwrap().path().to_str().unwrap().to_string();
            if test_file_name.ends_with(".out") {
                continue;
            }
            let output_file_name = test_file_name.replace(".txt", ".out");
            print!("\nTest file: {}\n", test_file_name);
            let test_output = std::fs::read(output_file_name).expect("Failed to read file");

            let file = file::File::open(&test_file_name).expect("Failed to open file");

            let result = file.parse();

            let mut actual_output = Vec::new();

            write!(actual_output, "{{")
                .context("Failed to write to {OUT_FILE_PATH}")
                .unwrap();
            for (i, (city, measurement)) in result.iter().enumerate() {
                write!(
                    actual_output,
                    "{}={:.1}/{:.1}/{:.1}",
                    city, measurement.min, measurement.avg, measurement.max
                )
                .context("Failed to write to {OUT_FILE_PATH}")
                .unwrap();

                if i != result.len() - 1 {
                    write!(actual_output, ", ")
                        .context("Failed to write to {OUT_FILE_PATH}")
                        .unwrap();
                };
            }
            write!(actual_output, "}}\n")
                .context("Failed to write to {OUT_FILE_PATH}")
                .unwrap();

            if actual_output != test_output {
                panic!(
                    "Test failed for file: {}, expected: {:?}, got: {:?}",
                    test_file_name,
                    std::str::from_utf8(&test_output),
                    std::str::from_utf8(&actual_output)
                );
            }

            print!("Test passed\n");
            print!("-----------------------------------\n");
        }
    }
}
