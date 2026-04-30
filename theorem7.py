import csv
import sys
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Dict, List, Tuple


EPS = 1e-9

BASE_DIR = Path(__file__).resolve().parent
INPUTS_DIR = BASE_DIR / "inputs"
LEGACY_INPUT_DIR = BASE_DIR / "input"
OUTPUTS_DIR = BASE_DIR / "outputs"


# ============================================================
# Data classes
# ============================================================

@dataclass(frozen=True)
class Job:
    id: str
    r: int
    d: int
    p: int


@dataclass
class ScheduledPiece:
    job_id: str
    start: int
    end: int


@dataclass
class MachinePiece:
    machine_id: str
    start: int
    end: int
    jobs: List[str]


# ============================================================
# CSV input
# ============================================================

def resolve_input_csv_path(filename: str) -> Path:
    input_path = Path(filename)

    if input_path.is_absolute():
        return input_path

    candidates = [
        INPUTS_DIR / input_path,
        BASE_DIR / input_path,
        Path.cwd() / input_path,
        LEGACY_INPUT_DIR / input_path,
    ]

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    return INPUTS_DIR / input_path


def parse_int_field(value: str, field_name: str, job_id: str | None = None) -> int:
    stripped_value = value.strip()

    try:
        parsed_value = int(stripped_value)
    except ValueError as exc:
        owner = f" for job {job_id}" if job_id else ""
        raise ValueError(f"{field_name}{owner} must be an integer.") from exc

    if str(parsed_value) != stripped_value:
        owner = f" for job {job_id}" if job_id else ""
        raise ValueError(f"{field_name}{owner} must be an integer.")

    return parsed_value


def read_input_from_csv(filename: str | Path) -> Tuple[int, List[Job]]:
    """
    Reads this CSV format:

    Busy Time
    Capacity
    2
    Job,Release,Deadline,processingTime
    A,3,6,1
    B,3,9,3
    """

    with open(filename, mode="r", newline="") as file:
        reader = csv.reader(file, skipinitialspace=True)
        rows = [row for row in reader if row]

    if len(rows) < 4:
        raise ValueError("CSV must contain title, Capacity, value, and job header.")

    if rows[1][0].strip() != "Capacity":
        raise ValueError("Second row must be: Capacity")

    g = parse_int_field(rows[2][0], "Capacity")

    if g <= 0:
        raise ValueError("Capacity must be a positive integer.")

    header = [col.strip() for col in rows[3]]
    required_header = ["Job", "Release", "Deadline", "processingTime"]

    if header != required_header:
        raise ValueError("Job header must be: Job,Release,Deadline,processingTime")

    jobs: List[Job] = []

    for row in rows[4:]:
        if len(row) < 4:
            continue

        job_id = row[0].strip()
        r = parse_int_field(row[1], "Release", job_id)
        d = parse_int_field(row[2], "Deadline", job_id)
        p = parse_int_field(row[3], "processingTime", job_id)

        if not job_id:
            raise ValueError("Every job must have a non-empty Job name.")

        if d <= r:
            raise ValueError(f"Job {job_id} is invalid: Deadline must be greater than Release.")

        if p <= 0:
            raise ValueError(f"Job {job_id} is invalid: processingTime must be positive.")

        if p > d - r:
            raise ValueError(f"Job {job_id} is infeasible: processingTime > Deadline - Release.")

        jobs.append(Job(job_id, r, d, p))

    return g, jobs


# ============================================================
# Interval helpers
# ============================================================

def merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
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


def intersection_length(intervals: List[Tuple[int, int]], r: int, d: int) -> int:
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

    if need <= EPS:
        return active

    active = merge_intervals(active)
    clipped = clip_intervals(active, r, d)

    new_intervals = []
    cursor = d

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

    if need > EPS and cursor > r + EPS:
        gap_length = cursor - r
        take = min(need, gap_length)

        new_intervals.append((cursor - take, cursor))
        need -= take

    if need > EPS:
        raise ValueError(f"Instance appears infeasible inside window [{r}, {d})")

    return merge_intervals(active + new_intervals)


# ============================================================
# Step 1: Unbounded-capacity schedule
# ============================================================

def compute_unbounded_active_intervals(jobs: List[Job]) -> List[Tuple[int, int]]:
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

    schedule: Dict[str, List[ScheduledPiece]] = {}

    for job in sorted(jobs, key=lambda j: j.d):
        remaining = job.p
        pieces: List[ScheduledPiece] = []

        possible_intervals = clip_intervals(active_intervals, job.r, job.d)

        for start, end in reversed(possible_intervals):
            if remaining <= EPS:
                break

            take = min(remaining, end - start)
            piece_start = end - take
            piece_end = end

            pieces.append(ScheduledPiece(job.id, piece_start, piece_end))
            remaining -= take

        if remaining > EPS:
            raise ValueError(f"Could not assign enough active time to job {job.id}.")

        schedule[job.id] = sorted(pieces, key=lambda piece: piece.start)

    return schedule


# ============================================================
# Step 2: Convert to bounded capacity g
# ============================================================

def build_interesting_intervals(
    unbounded_schedule: Dict[str, List[ScheduledPiece]]
) -> List[Tuple[int, int]]:

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
        running_jobs = sorted(
            jobs_running_on_interval(unbounded_schedule, start, end)
        )

        number_of_machines = ceil(len(running_jobs) / g)

        for machine_index in range(number_of_machines):
            chunk = running_jobs[machine_index * g: (machine_index + 1) * g]

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

def total_unbounded_busy_time(active_intervals: List[Tuple[int, int]]) -> int:
    return sum(end - start for start, end in active_intervals)


def total_bounded_busy_time(bounded_schedule: List[MachinePiece]) -> int:
    return sum(piece.end - piece.start for piece in bounded_schedule)


# ============================================================
# CSV outputs
# ============================================================

def save_input_jobs_csv(output_file: str, jobs: List[Job]) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Release", "Deadline", "processingTime"])

        for job in jobs:
            writer.writerow([job.id, job.r, job.d, job.p])


def save_unbounded_active_intervals_csv(
    output_file: str,
    active_intervals: List[Tuple[int, int]]
) -> None:

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Interval", "Start", "End", "Length"])

        for i, (start, end) in enumerate(active_intervals, start=1):
            writer.writerow([f"Interval {i}", start, end, end - start])


def save_unbounded_schedule_csv(
    output_file: str,
    unbounded_schedule: Dict[str, List[ScheduledPiece]]
) -> None:

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
    output_prefix: str = "theorem7",
    output_base_dir: str | Path = OUTPUTS_DIR
) -> Dict[str, str]:

    output_dir = Path(output_base_dir) / output_prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    output_files = {
        "input_jobs": output_dir / f"{output_prefix}_input_jobs.csv",
        "unbounded_active_intervals": output_dir / f"{output_prefix}_unbounded_active_intervals.csv",
        "unbounded_schedule": output_dir / f"{output_prefix}_unbounded_schedule.csv",
        "bounded_schedule": output_dir / f"{output_prefix}_bounded_schedule.csv",
        "summary": output_dir / f"{output_prefix}_summary.csv",
    }

    save_input_jobs_csv(str(output_files["input_jobs"]), jobs)
    save_unbounded_active_intervals_csv(str(output_files["unbounded_active_intervals"]), active_intervals)
    save_unbounded_schedule_csv(str(output_files["unbounded_schedule"]), unbounded_schedule)
    save_bounded_schedule_csv(str(output_files["bounded_schedule"]), bounded_schedule)
    save_summary_csv(str(output_files["summary"]), g, active_intervals, bounded_schedule)

    return {label: str(path) for label, path in output_files.items()}


# ============================================================
# Terminal output
# ============================================================

def print_results(
    jobs: List[Job],
    g: int,
    active_intervals: List[Tuple[int, int]],
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    bounded_schedule: List[MachinePiece]
) -> None:

    print("Machine capacity g:", g)

    print("\nJobs read from CSV:")
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
    # Usage:
    # python theorem7.py jobs.csv
    # Input files are read from the inputs directory by default.

    csv_file = resolve_input_csv_path(sys.argv[1] if len(sys.argv) > 1 else "jobs.csv")

    output_prefix = csv_file.stem

    # Read capacity and jobs from CSV.
    g, jobs = read_input_from_csv(csv_file)

    # Run the scheduling algorithm.
    active_intervals, unbounded_schedule, bounded_schedule = theorem_7_bounded_preemptive_schedule(
        jobs,
        g
    )

    # Print results.
    print_results(
        jobs,
        g,
        active_intervals,
        unbounded_schedule,
        bounded_schedule
    )

    # Save CSV output files.
    output_files = save_all_results_to_separate_csv_files(
        jobs,
        active_intervals,
        unbounded_schedule,
        bounded_schedule,
        g,
        output_prefix,
        OUTPUTS_DIR
    )

    print("\nResults saved to separate CSV files:")
    for label, filename in output_files.items():
        print(f"{label}: {filename}")
