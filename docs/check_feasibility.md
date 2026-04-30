# Feasibility Checker for Preemptive Busy-Time Scheduling

The `check_feasibility.py` program verifies whether a bounded preemptive busy-time schedule is valid with respect to a given input instance of jobs.


## What the Program Does

Given:

- An **input CSV** describing jobs
- A **bounded schedule CSV** (or output directory)

The checker validates that the schedule:

- Meets all job requirements
- Respects machine capacity constraints
- Does not violate scheduling rules

It outputs:

- **FEASIBLE** if all constraints are satisfied
- **NOT FEASIBLE** with detailed errors otherwise
- **Warnings** for suspicious but non-fatal issues

---

## Input Format

### 1. Job Input CSV

The input file must follow this exact format:

```csv
Busy Time
Capacity
2
Job,Release,Deadline,processingTime
A,3,6,1
B,3,9,3
...
```

### Constraints

- Deadline > Release
- processingTime > 0
- processingTime ≤ Deadline - Release

### 2. Bounded Schedule CSV

Expected format:

```
Machine,Start,End,Length,Jobs on Machine
M1,0,2,2,J1,J2
M2,0,2,2,J3
...
```

### 3. Optional Summary CSV

Used for consistency checks:

```csv
Metric,Value
Machine capacity g,2
Unbounded busy time,5
Bounded busy time,6
Bounded / Unbounded ratio,1.2
```

---

## How to Run

### Option 1: Pass output directory

```bash
python check_feasibility.py inputs/input1.csv outputs/input1_l2_preemptive
```

### Option 2: Pass bounded schedule directly

```bash
python check_feasibility.py inputs/input1.csv outputs/input1_l2_preemptive/input1_l2_preemptive_bounded_schedule.csv
```

### Option 3: Strict summary validation

```bash
python check_feasibility.py inputs/input1.csv outputs/input1_l2_preemptive --strict-summary
```

---

## Feasibility Checks Performed

1. Input Validation  
    - Correct CSV format
    - Valid job definitions
    - No duplicate job IDs

2. Output Format Validation  
    - Correct header
    - Valid integer fields
    - Non-empty schedule

3. Job Existence  
    - Every scheduled job must exist in input

4. Time Window Constraints  
    - Each scheduled interval must lie within `[release, deadline)`

5. No Parallel Execution of Same Job  
    - A job cannot run on multiple machines at the same time

6. Processing Time Completion  
    - Total scheduled time for each job must equal its required processing time

7. Machine Capacity Constraint  
    - Each row must have at most `g` jobs

8. Interval Consistency  
    - `Length == End - Start`

9. Additional Sanity Checks (Warnings)  
    - Negative times
    - Duplicate jobs in a row
    - Overlapping intervals on same machine label
    - Jobs never scheduled
    - Summary mismatches (unless strict mode enabled)

---

## Output

The program prints:

### Summary

```bash
Feasibility checker
===================
Input CSV: ...
Bounded schedule CSV: ...
Summary CSV: ...
Machine capacity g: ...
Jobs in input: ...
Schedule rows checked: ...
```

### Warnings (if any)

```bash
WARNING: Jobs never mentioned in output: A
```

### Errors (if any)

```bash
ERROR: Job 'B': scheduled for 1 total time, but processingTime is 2.
ERROR: Job 'C': runs in parallel on multiple machines.
```

### Final Result

```bash
Result: FEASIBLE
```

or

```bash
Result: NOT FEASIBLE
```

---

## Exit Codes

- `0` → Feasible
- `1` → Not feasible
- `2` → Checker failed (e.g., file errors)

---
