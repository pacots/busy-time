# Busy-Time Scheduling

This repository contains Python implementations of busy-time scheduling
algorithms.

## Project Structure

```text
busy-time/
+-- theorem7.py
+-- l2_preemptive_busy_time.py
+-- README.md
+-- docs/
|   +-- theorem7.md
|   +-- l2_preemptive_busy_time.md
+-- inputs/
|   +-- jobs_c2.csv
+-- outputs/
```

## Implementations

- `theorem7.py`: original bounded preemptive scheduling implementation.
  Documentation: [`docs/theorem7.md`](docs/theorem7.md).
- `l2_preemptive_busy_time.py`: L2 implementation of the paper's Section 4.4
  preemptive busy-time algorithm. Documentation:
  [`docs/l2_preemptive_busy_time.md`](docs/l2_preemptive_busy_time.md).

## Requirements

The scripts use only the Python standard library. Python 3.10 or newer is
recommended because the code uses `str | Path` type annotations.

## Input Format

Both scripts use the same CSV format:

```csv
Busy Time
Capacity
2
Job,Release,Deadline,processingTime
A,3,6,1
B,3,9,3
```

Input files are stored in `inputs/` by default.

## Run

```bash
python theorem7.py jobs_c2.csv
python l2_preemptive_busy_time.py jobs_c2.csv
```

Generated CSV reports are written under `outputs/`.
