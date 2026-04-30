# L2 Preemptive Busy-Time Algorithm

This document describes `l2_preemptive_busy_time.py`, a named implementation of
the preemptive busy-time algorithm from Section 4.4 of:

```text
Jessica Chang, Samir Khuller, Koyel Mukherjee.
LP rounding and combinatorial algorithms for minimizing active and busy time.
Journal of Scheduling, 2017.
```

The implemented part corresponds to:

- Theorem 6: exact greedy algorithm for preemptive busy time with unbounded
  parallelism.
- Theorem 7: bounded-capacity conversion that gives a 2-approximation for
  capacity `g`.

---

## How To Run

From the repository root:

```bash
python l2_preemptive_busy_time.py jobs_c2.csv
```

The script resolves relative input files from `inputs/` first, so this reads:

```text
inputs/jobs_c2.csv
```

By default, outputs are written under:

```text
outputs/jobs_c2_l2_preemptive/
```

You can choose a different output prefix:

```bash
python l2_preemptive_busy_time.py jobs_c2.csv --output-prefix my_run
```

---

## Input Format

The script uses the same CSV format as `theorem7.py`:

```csv
Busy Time
Capacity
2
Job,Release,Deadline,processingTime
A,3,6,1
B,3,9,3
```

Fields:

- `Capacity`: machine capacity `g`.
- `Job`: job identifier.
- `Release`: integer release time $r_j$.
- `Deadline`: integer deadline $d_j$.
- `processingTime`: integer required processing length $p_j$.

All numeric input fields must be integers. Decimal values such as `3.0` or `3.5` are rejected.

The script validates that each job is individually feasible:

```text
processingTime ≤ Deadline - Release
```

---

## Algorithm Implemented

The code intentionally follows the paper's Section 4.4 structure instead of the
older implementation in `theorem7.py`.

### Step 1: Unbounded Preemptive Busy Time

Function:

```python
compute_unbounded_preemptive_schedule(jobs)
```

This implements the greedy algorithm from Theorem 6.

The paper describes the greedy process by opening the latest interval required
by the earliest-deadline work, shrinking that interval, and repeating. The
script keeps the original time axis and applies the same idea without changing
coordinates.

At each iteration:

1. Consider jobs in nondecreasing deadline order.
2. For the current job, compute how much already-open active time lies inside its window $[r_j, d_j)$.
3. If that active time is not enough, add exactly the missing amount as late as possible inside $[r_j, d_j)$.
4. After all active intervals are chosen, assign each job to active time inside its window, using latest feasible active time first.

This is the same contraction argument represented on the uncompressed original timeline. The result is the unbounded schedule $S_{\infty}$, which is optimal when machine capacity is unbounded.

### Step 2: Bounded Capacity `g`

Function:

```python
convert_to_bounded_preemptive_schedule(unbounded_schedule, g)
```

This implements the conversion described after Theorem 6 and summarized by
Theorem 7.

The conversion:

1. Splits $S_{\infty}$ into interesting intervals. These are maximal intervals
   where the set of running jobs does not change.
2. For each interesting interval $I_i$, finds the jobs running in that interval.
3. Assigns those jobs to $ceil(n(I_i) / g)$ machines, filling each machine greedily up to capacity `g`.

The output is a feasible preemptive schedule whose busy time is at most twice the optimal bounded-capacity preemptive busy time.

---

## Output Files

For `jobs_c2.csv`, the default output folder is:

```text
outputs/jobs_c2_l2_preemptive/
```

Generated files:

- `jobs_c2_l2_preemptive_input_jobs.csv`: normalized copy of the input jobs.
- `jobs_c2_l2_preemptive_active_iterations.csv`: one row per greedy unbounded step, including the job considered, its deadline, and any newly added active interval.
- `jobs_c2_l2_preemptive_unbounded_active_intervals.csv`: merged $S_{\infty}$ intervals.
- `jobs_c2_l2_preemptive_unbounded_schedule.csv`: preemptive job pieces in the unbounded schedule.
- `jobs_c2_l2_preemptive_bounded_schedule.csv`: bounded-capacity machine pieces after applying Theorem 7.
- `jobs_c2_l2_preemptive_summary.csv`: busy-time metrics.

---

## Notes

- Jobs may be preempted.
- A job is never processed on more than one machine at the same time.
- Intervals are represented as half-open intervals: `[start, end)`.
- The bounded schedule uses local machine identifiers per interesting interval,
  such as `I003_M002`. This mirrors the paper's interval-by-interval assignment:
  the objective is total busy time, not minimizing the number of machine labels.
- The implementation uses only Python's standard library.

---