import csv
import sys
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Dict, List, Tuple


EPS = 1e-9


# ============================================================
# Data classes
# ============================================================

@dataclass(frozen=True)
class Job:
    """
    A preemptive job.

    id: job name
    r: release time
    d: deadline
    p: processing time
    """
    id: str
    r: int
    d: int
    p: int


@dataclass
class ScheduledPiece:
    """
    A piece of a preemptive job schedule.
    The job runs during [start, end).
    """
    job_id: str
    start: int
    end: int


@dataclass
class MachinePiece:
    """
    A bounded-capacity machine assignment.

    On [start, end), this machine runs the listed jobs.
    """
    machine_id: str
    start: int
    end: int
    jobs: List[str]


# ============================================================
# CSV input
# ============================================================

def read_input_from_csv(filename: str) -> Tuple[int, List[Job]]:
    """
    Reads the input CSV with the following exact format:
    
    Busy Time
    Capacity
    2
    Job, Release, Deadline, processingTime
    A, 1, 4, 2
    ...
    """
    jobs: List[Job] = []
    
    with open(filename, mode="r", newline="", encoding="utf-8-sig") as file:
        reader = csv.reader(file, skipinitialspace=True)
        
        # 1. Read and skip the title line (e.g., "Busy Time")
        title_row = next(reader, None)
        while title_row is not None and not any(title_row):
            title_row = next(reader, None)
            
        if not title_row:
            raise ValueError("The CSV file is empty.")
            
        # 2. Read "Capacity" label line
        capacity_label_row = next(reader, None)
        while capacity_label_row is not None and not any(capacity_label_row):
            capacity_label_row = next(reader, None)

        if not capacity_label_row or capacity_label_row[0].strip().lower() != "capacity":
            raise ValueError(
                f"Expected a line with 'Capacity'. Found: {capacity_label_row}"
            )

        capacity_value_row = next(reader, None)
        while capacity_value_row is not None and not any(capacity_value_row):
            capacity_value_row = next(reader, None)

        if not capacity_value_row:
            raise ValueError("Missing capacity value after 'Capacity' line.")

        try:
            capacity = int(capacity_value_row[0].strip())
        except ValueError:
            raise ValueError(
                f"Capacity must be an integer. Found: {capacity_value_row[0]}"
            )

        if capacity <= 0:
            raise ValueError("Machine capacity must be a positive integer.")


        # 3. Read headers from the third row
        headers = next(reader, None)
        while headers is not None and not any(headers):
            headers = next(reader, None)

        if not headers:
            raise ValueError("Missing header row in CSV (should be the third line).")
            
        clean_headers = [str(h).strip() for h in headers if h.strip()]
            
        required_columns = {"Job", "Release", "Deadline", "processingTime"}
        if not required_columns.issubset(clean_headers):
            raise ValueError(
                f"CSV headers (third line) must contain the columns: Job, Release, Deadline, processingTime.\n"
                f"Instead, Python found these exact headers: {clean_headers}\n"
                f"Check your CSV for typos, missing lines, or hidden characters."
            )

        # 4. Read jobs from the fourth row onwards
        dict_reader = csv.DictReader(file, fieldnames=clean_headers, skipinitialspace=True)
        
        for row_num, row in enumerate(dict_reader, start=4):
            if not row.get("Job") or not row["Job"].strip():
                continue

            try:
                job_id = row["Job"].strip()
                r = int(row["Release"].strip())
                d = int(row["Deadline"].strip())
                p = int(row["processingTime"].strip())
            except ValueError as e:
                raise ValueError(f"Error reading numbers on row {row_num} (Job {row.get('Job')}): {e}")

            if d <= r:
                raise ValueError(f"Job {job_id} is invalid because Deadline must be greater than Release.")

            if p <= 0:
                raise ValueError(f"Job {job_id} is invalid because processingTime must be positive.")

            if p > d - r:
                raise ValueError(f"Job {job_id} is infeasible because processingTime ({p}) > Deadline - Release ({d - r}).")

            jobs.append(Job(job_id, r, d, p))

    return capacity, jobs


# ============================================================
# Interval helper functions
# ============================================================

def merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """
    Merges overlapping or touching intervals.

    Example:
    [(1, 3), (3, 5), (7, 8)] becomes [(1, 5), (7, 8)]
    """
    if not intervals:
        return []

    intervals = sorted(intervals)
    merged = [intervals[0]]

    for start, end in intervals[1:]:
        last_start, last_end = merged[-1]

        if start <= last_end + EPS:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))

    return merged


def intersection_length(
    intervals: List[Tuple[int, int]],
    r: int,
    d: int
) -> int:
    """
    Returns the total length of intervals that intersect [r, d).
    """
    total = 0

    for start, end in intervals:
        left = max(start, r)
        right = min(end, d)

        if right > left + EPS:
            total += right - left

    return total


def clip_intervals(
    intervals: List[Tuple[int, int]],
    r: int,
    d: int
) -> List[Tuple[int, int]]:
    """
    Returns only the parts of intervals that lie inside [r, d).
    """
    clipped = []

    for start, end in intervals:
        left = max(start, r)
        right = min(end, d)

        if right > left + EPS:
            clipped.append((left, right))

    return clipped


def add_latest_inactive_time(
    active: List[Tuple[int, int]],
    r: int,
    d: int,
    need: int
) -> List[Tuple[int, int]]:
    """
    Adds 'need' amount of active time inside [r, d), as late as possible.
    """
    if need <= EPS:
        return active

    active = merge_intervals(active)
    clipped = clip_intervals(active, r, d)

    new_intervals = []
    cursor = d

    # Scan backward through currently active intervals.
    for start, end in reversed(clipped):
        gap_start = end
        gap_end = cursor

        if gap_end > gap_start + EPS:
            gap_length = gap_end - gap_start
            take = min(need, gap_length)

            new_intervals.append((gap_end - take, gap_end))
            need -= take

            if need <= EPS:
                break

        cursor = start

    # Gap before the first active interval.
    if need > EPS and cursor > r + EPS:
        gap_length = cursor - r
        take = min(need, gap_length)

        new_intervals.append((cursor - take, cursor))
        need -= take

    if need > EPS:
        raise ValueError(
            f"Instance appears infeasible: not enough room in window [{r}, {d})"
        )

    return merge_intervals(active + new_intervals)


# ============================================================
# Step 1: Solve unbounded-capacity preemptive busy-time problem
# ============================================================

def compute_unbounded_active_intervals(jobs: List[Job]) -> List[Tuple[int, int]]:
    """
    Computes the active intervals S_infinity for the unbounded-capacity schedule.
    """
    active: List[Tuple[int, int]] = []

    for job in sorted(jobs, key=lambda j: j.d):
        already_available = intersection_length(active, job.r, job.d)
        deficit = job.p - already_available

        if deficit > EPS:
            active = add_latest_inactive_time(active, job.r, job.d, deficit)

    return merge_intervals(active)


def assign_jobs_to_unbounded_schedule(
    jobs: List[Job],
    active_intervals: List[Tuple[int, int]]
) -> Dict[str, List[ScheduledPiece]]:
    """
    Assigns each job to exactly p units inside the active intervals.
    """
    schedule: Dict[str, List[ScheduledPiece]] = {}

    for job in sorted(jobs, key=lambda j: j.d):
        remaining = job.p
        pieces: List[ScheduledPiece] = []

        possible_intervals = clip_intervals(active_intervals, job.r, job.d)

        for start, end in reversed(possible_intervals):
            if remaining <= EPS:
                break

            interval_length = end - start
            take = min(remaining, interval_length)

            piece_start = end - take
            piece_end = end

            pieces.append(ScheduledPiece(job.id, piece_start, piece_end))
            remaining -= take

        if remaining > EPS:
            raise ValueError(
                f"Could not assign enough active time to job {job.id}."
            )

        schedule[job.id] = sorted(pieces, key=lambda piece: piece.start)

    return schedule


# ============================================================
# Step 2: Convert unbounded schedule to bounded capacity g
# ============================================================

def build_interesting_intervals(
    unbounded_schedule: Dict[str, List[ScheduledPiece]]
) -> List[Tuple[int, int]]:
    """
    Breaks time into smaller intervals where the set of running jobs is constant.
    """
    points = set()

    for pieces in unbounded_schedule.values():
        for piece in pieces:
            points.add(piece.start)
            points.add(piece.end)

    points = sorted(points)

    intervals = []

    for start, end in zip(points, points[1:]):
        if end > start + EPS:
            intervals.append((start, end))

    return intervals


def jobs_running_on_interval(
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    start: int,
    end: int
) -> List[str]:
    """
    Returns all jobs that are running throughout [start, end).
    """
    running_jobs = []

    for job_id, pieces in unbounded_schedule.items():
        for piece in pieces:
            if piece.start <= start + EPS and piece.end >= end - EPS:
                running_jobs.append(job_id)
                break

    return running_jobs


def theorem_7_bounded_preemptive_schedule(
    jobs: List[Job],
    g: int
) -> Tuple[
    List[Tuple[int, int]],
    Dict[str, List[ScheduledPiece]],
    List[MachinePiece]
]:
    """
    Main Theorem 7 implementation.
    """
    if g <= 0:
        raise ValueError("Machine capacity g must be positive.")

    active_intervals = compute_unbounded_active_intervals(jobs)

    unbounded_schedule = assign_jobs_to_unbounded_schedule(
        jobs,
        active_intervals
    )

    interesting_intervals = build_interesting_intervals(unbounded_schedule)

    bounded_schedule: List[MachinePiece] = []

    for start, end in interesting_intervals:
        running_jobs = jobs_running_on_interval(
            unbounded_schedule,
            start,
            end
        )

        running_jobs = sorted(running_jobs)

        number_of_machines = ceil(len(running_jobs) / g)

        for machine_index in range(number_of_machines):
            chunk = running_jobs[
                machine_index * g : (machine_index + 1) * g
            ]

            if chunk:
                bounded_schedule.append(
                    MachinePiece(
                        machine_id=f"M{machine_index + 1}",
                        start=start,
                        end=end,
                        jobs=chunk
                    )
                )

    return active_intervals, unbounded_schedule, bounded_schedule


# ============================================================
# Busy-time calculations
# ============================================================

def total_unbounded_busy_time(
    active_intervals: List[Tuple[int, int]]
) -> int:
    """
    Total busy time in the unbounded-capacity schedule.
    """
    return sum(end - start for start, end in active_intervals)


def total_bounded_busy_time(
    bounded_schedule: List[MachinePiece]
) -> int:
    """
    Total busy time in the bounded-capacity schedule.
    """
    return sum(piece.end - piece.start for piece in bounded_schedule)


# ============================================================
# Separate CSV output files
# ============================================================

def save_input_jobs_csv(output_file: str, jobs: List[Job]) -> None:
    """
    Saves the cleaned input jobs into a separate CSV file.
    """
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Release", "Deadline", "processingTime"])

        for job in jobs:
            writer.writerow([job.id, job.r, job.d, job.p])


def save_unbounded_active_intervals_csv(
    output_file: str,
    active_intervals: List[Tuple[int, int]]
) -> None:
    """
    Saves the unbounded active intervals S_infinity into a separate CSV file.
    """
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Interval", "Start", "End", "Length"])

        for i, (start, end) in enumerate(active_intervals, start=1):
            writer.writerow([f"Interval {i}", start, end, end - start])


def save_unbounded_schedule_csv(
    output_file: str,
    unbounded_schedule: Dict[str, List[ScheduledPiece]]
) -> None:
    """
    Saves the unbounded preemptive job schedule into a separate CSV file.
    """
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Start", "End", "Length"])

        for job_id, pieces in sorted(unbounded_schedule.items()):
            for piece in pieces:
                writer.writerow([
                    job_id,
                    piece.start,
                    piece.end,
                    piece.end - piece.start
                ])


def save_bounded_schedule_csv(
    output_file: str,
    bounded_schedule: List[MachinePiece]
) -> None:
    """
    Saves the bounded-g machine schedule into a separate CSV file.
    """
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Machine", "Start", "End", "Length", "Jobs on Machine"])

        for piece in bounded_schedule:
            writer.writerow([
                piece.machine_id,
                piece.start,
                piece.end,
                piece.end - piece.start,
                ", ".join(piece.jobs)
            ])


def save_summary_csv(
    output_file: str,
    g: int,
    active_intervals: List[Tuple[int, int]],
    bounded_schedule: List[MachinePiece]
) -> None:
    """
    Saves summary values into a separate CSV file.
    """
    unbounded_busy_time = total_unbounded_busy_time(active_intervals)
    bounded_busy_time = total_bounded_busy_time(bounded_schedule)

    ratio = ""
    if unbounded_busy_time > 0:
        ratio = bounded_busy_time / unbounded_busy_time

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Machine capacity g", g])
        writer.writerow(["Unbounded busy time", unbounded_busy_time])
        writer.writerow(["Bounded busy time", bounded_busy_time])
        writer.writerow(["Bounded / Unbounded ratio", ratio])


def save_all_results_to_separate_csv_files(
    jobs: List[Job],
    active_intervals: List[Tuple[int, int]],
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    bounded_schedule: List[MachinePiece],
    g: int,
    output_prefix: str = "theorem7"
) -> Dict[str, str]:
    """
    Saves all results into separate CSV files.

    Returns a dictionary of file labels to filenames.
    """
    output_files = {
        "input_jobs": f"{output_prefix}_input_jobs.csv",
        "unbounded_active_intervals": f"{output_prefix}_unbounded_active_intervals.csv",
        "unbounded_schedule": f"{output_prefix}_unbounded_schedule.csv",
        "bounded_schedule": f"{output_prefix}_bounded_schedule.csv",
        "summary": f"{output_prefix}_summary.csv",
    }

    save_input_jobs_csv(output_files["input_jobs"], jobs)
    save_unbounded_active_intervals_csv(
        output_files["unbounded_active_intervals"],
        active_intervals
    )
    save_unbounded_schedule_csv(
        output_files["unbounded_schedule"],
        unbounded_schedule
    )
    save_bounded_schedule_csv(
        output_files["bounded_schedule"],
        bounded_schedule
    )
    save_summary_csv(
        output_files["summary"],
        g,
        active_intervals,
        bounded_schedule
    )

    return output_files


# ============================================================
# Print output to terminal
# ============================================================

def print_results(
    jobs: List[Job],
    active_intervals: List[Tuple[int, int]],
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    bounded_schedule: List[MachinePiece]
) -> None:
    """
    Prints the results in a readable format.
    """

    print("Jobs read from CSV:")
    for job in jobs:
        print(job)

    print("\nUnbounded active intervals S_infinity:")
    for interval in active_intervals:
        print(interval)

    print("\nUnbounded preemptive job schedule:")
    for job_id, pieces in sorted(unbounded_schedule.items()):
        formatted_pieces = [(piece.start, piece.end) for piece in pieces]
        print(job_id, formatted_pieces)

    print("\nBounded-g machine schedule:")
    for piece in bounded_schedule:
        print(
            piece.machine_id,
            f"[{piece.start}, {piece.end})",
            "jobs:",
            piece.jobs
        )

    print("\nUnbounded busy time:", total_unbounded_busy_time(active_intervals))
    print("Bounded busy time:", total_bounded_busy_time(bounded_schedule))


# ============================================================
# Main program
# ============================================================

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "jobs.csv"
    output_prefix = Path(csv_file).stem

    # Read capacity 'g' and input jobs directly from the CSV
    g, jobs = read_input_from_csv(csv_file)

    print(f"Loaded Machine Capacity (g) from CSV: {g}\n")

    # Run Theorem 7 scheduling algorithm
    active_intervals, unbounded_schedule, bounded_schedule = theorem_7_bounded_preemptive_schedule(jobs, g)

    # Print results in terminal
    print_results(jobs, active_intervals, unbounded_schedule, bounded_schedule)

    # Save results into different CSV files
    output_files = save_all_results_to_separate_csv_files(
        jobs, active_intervals, unbounded_schedule, bounded_schedule, g, output_prefix
    )

    print("\nResults saved to separate CSV files:")
    for label, filename in output_files.items():
        print(f"{label}: {filename}")