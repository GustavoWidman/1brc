#![feature(portable_simd, slice_as_chunks)]
use anyhow::Context;
use std::io::Write;

mod file;
mod hashmap;
mod measurement;

pub fn perform_calculations_only() -> anyhow::Result<()> {
    file::File::open("1b_measurements.txt")
        .context("Failed to open measurements.txt")?
        .parse();

    Ok(())
}

pub fn perform_full_challenge() -> anyhow::Result<()> {
    let file =
        file::File::open("1b_measurements.txt").context("Failed to open measurements.txt")?;
    let measurements = file.parse();

    // print the final measurements
    let mut output =
        std::fs::File::create("output.json").context("Failed to create output.json")?;

    write!(output, "{{").context("Failed to write to output.json")?;
    for (i, (city, measurement)) in measurements.iter().enumerate() {
        write!(
            output,
            "\"{}\": \"{:.1}/{:.1}/{:.1}\"",
            city, measurement.min, measurement.avg, measurement.max
        )
        .context("Failed to write to output.json")?;

        if i != measurements.len() - 1 {
            write!(output, ",").context("Failed to write to output.json")?;
        };
    }
    write!(output, "}}").context("Failed to write to output.json")?;

    Ok(())
}
