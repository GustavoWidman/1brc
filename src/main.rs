#![feature(portable_simd, slice_as_chunks)]
use anyhow::Context;
use std::io::Write;

mod file;
mod hashmap;
mod measurement;

fn main() -> anyhow::Result<()> {
    if std::fs::metadata("output.json").is_ok() {
        std::fs::remove_file("output.json").context("Failed to remove output.json")?;
    }

    let num_workers = rayon::current_num_threads();
    println!("Number of workers: {}", num_workers);

    let start = std::time::Instant::now();

    let file =
        file::File::open("1b_measurements.txt").context("Failed to open measurements.txt")?;

    let measurements = file.parse();

    println!("Calculations took {:?}", start.elapsed());

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

    println!("Full took {:?}", start.elapsed());

    Ok(())
}
