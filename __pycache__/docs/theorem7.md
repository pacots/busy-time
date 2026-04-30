# Theorem 7 Implementation

This document describes `theorem7.py`, the original implementation in this
repository. It reads a set of jobs from CSV, computes an unbounded-capacity
preemptive schedule, converts it into a bounded-capacity schedule with machine
capacity `g`, prints the result, and writes CSV reports.

## How To Run

From the repository root:

```bash
python theorem7.py jobs_c2.csv
```

Relative input filenames are resolved from `inputs/` first, so this reads:

```text
inputs/jobs_c2.csv
```

If no argument is provided, the script tries to read:

```text
inputs/jobs.csv
```

## Input Format

The expected CSV format is:

```csv
Busy Time
Capacity
2
Job,Release,Deadline,processingTime
A,3,6,1
B,3,9,3
```

Fields:

- `Capacity`: maximum number of simultaneous jobs per machine, called `g`.
- `Job`: job identifier.
- `Release`: integer earliest time at which the job may start.
- `Deadline`: integer latest time by which the job must finish.
- `processingTime`: integer required processing length.

All numeric input fields must be integers. Decimal values such as `3.0` or
`3.5` are rejected.

The parser validates that every job has a non-empty name, positive processing
time, a deadline greater than its release time, and enough room in its window:

```text
processingTime <= Deadline - Release
```

## Output Files

For `jobs_c2.csv`, the script writes to:

```text
outputs/jobs_c2/
```

Generated files:

- `jobs_c2_input_jobs.csv`: normalized copy of the parsed input jobs.
- `jobs_c2_unbounded_active_intervals.csv`: active intervals in the unbounded
  schedule.
- `jobs_c2_unbounded_schedule.csv`: preemptive pieces assigned to each job.
- `jobs_c2_bounded_schedule.csv`: bounded-capacity machine schedule.
- `jobs_c2_summary.csv`: busy-time metrics and ratio.

## Code Structure

All implementation is in `theorem7.py`.

### Constants and Paths

```python
EPS = 1e-9

BASE_DIR = Path(__file__).resolve().parent
INPUTS_DIR = BASE_DIR / "inputs"
LEGACY_INPUT_DIR = BASE_DIR / "input"
OUTPUTS_DIR = BASE_DIR / "outputs"
```

- `EPS` is kept as a small numerical tolerance in interval comparisons.
- `BASE_DIR` points to the repository root.
- `INPUTS_DIR` is the default input folder.
- `LEGACY_INPUT_DIR` supports an older `input/` folder name.
- `OUTPUTS_DIR` is where result CSV files are written.

### Data Classes

`Job` stores one input job:

```python
@dataclass(frozen=True)
class Job:
    id: str
    r: int
    d: int
    p: int
```

`ScheduledPiece` stores one preemptive piece of a job:

```python
@dataclass
class ScheduledPiece:
    job_id: str
    start: int
    end: int
```

`MachinePiece` stores one interval assigned to one bounded-capacity machine:

```python
@dataclass
class MachinePiece:
    machine_id: str
    start: int
    end: int
    jobs: List[str]
```

## Algorithm Flow

### 1. Resolve and Read Input

`resolve_input_csv_path(filename)` searches for relative input files in this
order:

1. `inputs/<filename>`
2. repository root
3. current working directory
4. `input/<filename>`

`read_input_from_csv(filename)` returns:

```python
Tuple[int, List[Job]]
```

The integer is `g`; the list contains parsed jobs.

### 2. Build Unbounded Active Intervals

`compute_unbounded_active_intervals(jobs)` processes jobs by increasing
deadline. For each job, it computes how much active time already exists inside
the job window `[r, d)`.

If the existing active time is insufficient, it calls:

```python
add_latest_inactive_time(active, r, d, deficit)
```

This adds the missing active time as late as possible inside the job window.

### 3. Assign Jobs to the Unbounded Schedule

`assign_jobs_to_unbounded_schedule(jobs, active_intervals)` assigns each job to
the active intervals that overlap its window. The assignment goes from latest
feasible active time backward, and jobs may be split into multiple pieces.

### 4. Convert to Bounded Capacity

`build_interesting_intervals(unbounded_schedule)` collects every start and end
point from the unbounded schedule, sorts them, and creates elementary intervals.

`jobs_running_on_interval(unbounded_schedule, start, end)` finds all jobs that
cover one elementary interval.

`theorem_7_bounded_preemptive_schedule(jobs, g)` then groups running jobs into
chunks of size `g` for each elementary interval. Each chunk becomes one
`MachinePiece`.

The number of machine pieces opened for an interval is:

```python
ceil(number_of_running_jobs / g)
```

## Metrics

The script reports:

- `total_unbounded_busy_time(active_intervals)`: total length of unbounded
  active intervals.
- `total_bounded_busy_time(bounded_schedule)`: sum of all bounded machine-piece
  lengths.
- `Bounded / Unbounded ratio`: bounded busy time divided by unbounded busy time.

For the included `jobs_c2.csv`, the summary is:

```csv
Metric,Value
Machine capacity g,2
Unbounded busy time,24.0
Bounded busy time,157.0
Bounded / Unbounded ratio,6.541666666666667
```

## Notes

- Time values are handled as integers.
- Intervals are represented as half-open intervals: `[start, end)`.
- Jobs may be preempted.
- Output files are overwritten when the same input filename is run again.
