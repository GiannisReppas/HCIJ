# HCIJ — Hardware-Conscious Interval Join

> A high-performance parallel interval join algorithm for modern, multi-socket, multi-core CPUs — with explicit SIMD vectorization, NUMA-aware memory management, and output-aware load balancing.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Building](#building)
  - [Datasets](#datasets)
- [Usage](#usage)
  - [Options](#options)
  - [Examples](#examples)

---

## Overview

An **interval join** matches pairs of tuples from two relations whose intervals overlap. It is one of the most frequently used operations in temporal and spatial databases — e.g., joining employee contract periods with project timelines, or correlating a patient's medication window with a high heart-rate period.

HCIJ is designed from the ground up to exploit what modern CPUs actually offer:

| Challenge | HCIJ's Answer |
|---|---|
| Poor SIMD utilization in plane-sweep joins | Explicit vectorized forward scans |
| Load imbalance across cores | Output-aware scheduling via the Forward Scan Map (FSMap) |
| Memory stalls from unbounded grouping | Cache-sized grouping with an empirically tuned upper bound |
| Remote NUMA memory accesses | Selective relation replication per CPU socket |

Compared to state-of-the-art solutions (optFS, LEBI), HCIJ achieves **1.2× – 3.2× faster** execution while maintaining a **significantly smaller memory footprint**.

---

## Key Features

- **Forward Scan Map (FSMap):** A novel data structure that encodes the exact memory region each forward scan must cover, enabling O(1)-cost output cardinality estimation and precise load balancing — without any domain partitioning.
- **Explicit SIMD Vectorization:** Redesigned forward scans using 256-bit (AVX2) or 512-bit (AVX-512) SIMD registers on x86, and 128-bit NEON on ARM — overcoming the data-dependency barriers that prevent compiler auto-vectorization.
- **FS-based Scheduling:** Distributes forward scan work across threads proportionally to expected output size, avoiding the instability of domain-partitioning strategies under skewed data.
- **Cache-sized Grouping:** Caps group sizes to stay within L1d cache capacity, eliminating the memory stall cliff that appears when group intervals spill to DRAM.
- **NUMA Awareness:** Replicates input relations per CPU socket for memory-intensive joins, reducing remote NUMA accesses by up to 40% and recovering up to 1.3× performance on dual-socket servers.
- **Self-tuning:** Automatically selects the optimal execution path (FS-based scheduling vs. direct border execution) and toggles NUMA replication based on measured group-size statistics.
- **Portable:** Validated on x86-64 (Intel Xeon), ARM (Graviton3), and RISC-V (Milk-V Pioneer) architectures.

---

## How It Works

HCIJ replaces the traditional Domain-based Partitioning paradigm with a three-phase pipeline:

```
1. Border Discovery   — plane sweep to divide R ∪ S into t equal-cardinality sets
2. FSMap Creation     — parallel construction of the Forward Scan Map (each thread fills its region)
3. FS-based Scheduling — assign FSMap entries to threads by actual scan cost, then execute in parallel
```

The FSMap stores, for each group of consecutive intervals, the exact start and end of the memory region that must be forward-scanned to produce all matches. Because scan lengths are known before execution, work can be distributed with near-perfect balance — independent of interval distribution in the time domain.

---

## Getting Started

### Prerequisites

| Dependency | Notes |
|---|---|
| `g++` ≥ 11 | Compiled and tested with g++ 11.4 |
| `libtbb` | Intel Threading Building Blocks — for `tbb::parallel_sort`. Can be replaced with `std::sort` (see Makefile comments). |
| `libnuma` | For NUMA-aware memory allocation (`-lnuma`). Required on multi-socket servers. |
| AVX-512 or AVX2 | Use `-mavx512f` (default) or switch to `-mavx2` and enable the commented 256-bit SIMD paths in the source. |

### Building

```bash
cd HCIJ
make all
```

> **Note:** The Makefile uses `-mavx512f` by default. If your CPU does not support AVX-512, switch to `-mavx2` and uncomment the corresponding 256-bit SIMD lines in the source. The `-fno-omit-frame-pointer -g` flags are included for VTune profiling and can be safely removed for production builds.

### Datasets

The input datasets used in the paper are bundled as split compressed archives at `github.com/GiannisReppas/HCIJ.git` inputs folder:

```bash
# Reassemble and extract
cat inputs/inputs.tar.xz.part_* > inputs/inputs.tar.xz
tar -xJvf inputs/inputs.tar.xz
```

This will populate the `inputs/` directory with CSV files for the datasets. Each file contains intervals represented as `(start, end)` pairs.

---

## Usage

```
./ij [OPTIONS] <relation_R.csv> <relation_S.csv>
```

Run `./ij` with no arguments to print the full help message.

### Options

| Flag | Values | Description |
|---|---|---|
| `-m` | `xor`, `simd_xor` | Join mode. `xor` uses scalar execution; `simd_xor` enables explicit SIMD vectorization. |
| `-t` | integer | Number of threads to use. |
| `-g` | integer | Maximum group size for cache-sized grouping (recommended: `500`). Omit to disable grouping upper bound. |
| `-n` | `closest` | Enable NUMA-aware execution with relation replication (requires `libnuma`). |
| `-d` | *(flag)* | Use Domain-based Partitioning instead of FS-based scheduling. |

### Examples

**Recommended — FS-based scheduling with SIMD, cache grouping, and NUMA awareness (24 threads):**
```bash
./ij -m simd_xor -g 500 -n closest -t 24 \
    ./inputs/RequestsR18500K.csv \
    ./inputs/RequestsS18500K.csv
```

**Domain-based Partitioning baseline — scalar, no group bound (24 threads):**
```bash
./ij -m xor -t 24 -d \
    ./inputs/RequestsR18500K.csv \
    ./inputs/RequestsS18500K.csv
```

**Single-threaded scalar run (for profiling or comparison):**
```bash
./ij -m xor \
    ./inputs/DigR.csv \
    ./inputs/DigS.csv
```
