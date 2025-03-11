# 🚀 **1BRC - One Billion Row Challenge (Rust)** 🚀

[![Rust](https://img.shields.io/badge/Rust-🦀-orange?logo=rust)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen)](#)
[![Benchmarks](https://img.shields.io/badge/benchmarks-2.3s-blue)](#)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

### **1BRC** - One Billion Row Challenge

This repository contains a Rust implementation of the **One Billion Row Challenge**. The goal is to efficiently parse and process a large dataset with one billion rows, leveraging Rust's performance capabilities.
The challenge is to read a text file with 1 billion rows, each containing a mapping of a string to a floating point number (this is meant to signify a key-value pair, where the key is a weather station and the value is the temperature registered by that station at any given time). Once read, the program should calculate the minimum, maximum, and average temperature for each weather station and output the results into a JSON file (`output.json`), with the mappings being the weather station name and the values being a string with the format `min/max/avg`.

#### **Input Format**

The input should look a little like this:

```csv
...
Juba;9.2
Dar es Salaam;26.1
Honiara;22.4
San Salvador;6.9
Nashville;21.9
Vientiane;29.4
Edinburgh;22
Gaborone;37.2
...
```

#### **Output Format**

The output should look like this:

```json
{
  ...
  "Kankan": "-22.2/26.5/76.4",
  "Kano": "-35.8/26.4/83.6",
  "Kansas City": "-23.0/12.5/45.2",
  "Karachi": "-26.4/26.0/77.2",
  "Karonga": "-23.9/24.4/72.7",
  "Kathmandu": "-42.6/18.3/75.2",
  "Khartoum": "-14.2/29.9/80.4",
  "Kingston": "-34.3/27.4/86.2",
  "Kinshasa": "-17.4/25.3/71.3",
  ...
}
```

---

### ⚡ **Blazing fast SIMD-powered data cruncher!** ⚡

|  | **Cold Cache** | **Warm Cache** |
|--|----------------|----------------|
| ⏱️ **Performance** | 6-10 seconds   | 2.3-2.5 seconds  |

> **Latest Benchmarks:**
>
> - **Calculations only:** ~2.35s
> - **Full challenge:** ~2.36s

Benchmarks were run on my MacBook Pro 14" M3 Pro with 36GB Unified Memory and 14 CPU cores (10 performance, 4 efficiency).

---

## ✨ Features

- SIMD acceleration for ultra-fast parsing (responsible for around 10-20% of the performance gain)
- Multi-threaded processing (responsible for most of the performance gain) using [`rayon`](https://crates.io/crates/rayon)
- Optimized HashMap for fast lookups using [`hashbrown`](https://crates.io/crates/hashbrown)
- Optimized float32 parsing using [`fast-float2`](https://crates.io/crates/fast-float2)
- Efficient memory management
- Handles **1 billion+** rows efficiently and quickly
- Benchmark suite with Criterion for accurate performance measurements

---

## 🚀 Usage

```bash
# Run in release mode
cargo run --release

# Run benchmarks
cargo bench

# Generate sample data (e.g., 1000 rows)
cargo run --example generate 1000

# To generate the 1B challenge data, use:
cargo run --example generate 1000000000
```

---

## 📂 Project Structure

- `src/` - Core source code
- `benches/` - Benchmarks
- `examples/` - Data generators
- `1b_measurements.txt` - Input data (ignored in git)

---

## ❤️ Contributing

PRs welcome! Please benchmark your changes.

---

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
