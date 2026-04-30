# Busy-Time Scheduling

This repository contains Python implementations of busy-time scheduling algorithms.

## Project Structure

```text
busy-time/
+-- theorem7.py
+-- l2_preemptive_busy_time.py
+-- check_feasibility
+-- README.md
+-- docs/
|   +-- theorem7.md
|   +-- l2_preemptive_busy_time.md
+-- inputs/
|   +-- input1.csv
|   +-- jobs_c2.csv
+-- outputs/
```

## Implementations

Working:

- `l2_preemptive_busy_time.py`: L2 implementation of the paper's Section 4.4 preemptive busy-time algorithm.  
  Documentation: [`docs/l2_preemptive_busy_time.md`](docs/l2_preemptive_busy_time.md).
- `check_feasibility.py`: Verifies whether a bounded preemptive busy-time schedule is valid with respect to a given input instance of jobs.  
  Documentation: [`docs/check_feasibility.md`](docs/check_feasibility.md).

Currently Worning on:

- `local_search.py`
- `local_search_busy_time.py`

Archive:

- `theorem7.py`: Original bounded preemptive scheduling implementation.  
  Documentation: [`docs/theorem7.md`](docs/theorem7.md).
- `theorem7_scheduler_separate_outputs.py`


## Requirements

The scripts use only the Python standard library. Python 3.10 or newer is recommended because the code uses `str | Path` type annotations.

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

`Capacity`, `Release`, `Deadline`, and `processingTime` must all be integers. Decimal values such as `3.0` or `3.5` are rejected.

## Run

```bash
python theorem7.py jobs_c2.csv
python l2_preemptive_busy_time.py jobs_c2.csv
```

Generated CSV reports are written under `outputs/`.
