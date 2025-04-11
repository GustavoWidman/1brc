use anyhow::Context;
use std::io::Write;

mod file;
mod hashmap;
mod measurement;

pub static NUM_WORKERS: usize = 14;
pub static STATIONS_IN_DATASET: usize = 413;
pub static IN_FILE_PATH: &str = "./measurements.txt";
pub static OUT_FILE_PATH: &str = "./output.out";

#[inline(always)]
pub fn perform_calculations_only() -> anyhow::Result<()> {
    file::File::open(IN_FILE_PATH)
        .context(format!("Failed to open {IN_FILE_PATH}"))?
        .parse();

    Ok(())
}

#[inline(always)]
pub fn perform_full_challenge() -> anyhow::Result<()> {
    let file = file::File::open(IN_FILE_PATH).context(format!("Failed to open {IN_FILE_PATH}"))?;
    let measurements = file.parse();

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

    Ok(())
}
