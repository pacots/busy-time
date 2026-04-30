import argparse
import csv
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


EPS = 1e-9

BASE_DIR = Path(__file__).resolve().parent
INPUTS_DIR = BASE_DIR / "inputs"
LEGACY_INPUT_DIR = BASE_DIR / "input"
OUTPUTS_DIR = BASE_DIR / "outputs"


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
class ActiveIteration:
    iteration: int
    deadline: int
    added_intervals: str
    added_length: int
    considered_jobs: List[str]


@dataclass
class MachinePiece:
    machine_id: str
    start: int
    end: int
    jobs: List[str]


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

    header = [column.strip() for column in rows[3]]
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
        if p > d - r + EPS:
            raise ValueError(f"Job {job_id} is infeasible: processingTime > Deadline - Release.")

        jobs.append(Job(job_id, r, d, p))

    return g, jobs


def merge_intervals(intervals: Iterable[Tuple[int, int]]) -> List[Tuple[int, int]]:
    sorted_intervals = sorted(intervals)
    if not sorted_intervals:
        return []

    merged = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + EPS:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))

    return merged


def interval_length(intervals: Iterable[Tuple[int, int]]) -> int:
    return sum(end - start for start, end in intervals)


def intersection_length(
    intervals: List[Tuple[int, int]],
    r: int,
    d: int,
) -> int:
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
    d: int,
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
    need: int,
) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    if need <= EPS:
        return active, []

    active = merge_intervals(active)
    clipped = clip_intervals(active, r, d)
    added = []
    cursor = d

    for start, end in reversed(clipped):
        gap_start = end
        gap_end = cursor

        if gap_end > gap_start + EPS:
            gap_length = gap_end - gap_start
            take = min(need, gap_length)
            added.append((gap_end - take, gap_end))
            need -= take

            if need <= EPS:
                break

        cursor = start

    if need > EPS and cursor > r + EPS:
        gap_length = cursor - r
        take = min(need, gap_length)
        added.append((cursor - take, cursor))
        need -= take

    if need > EPS:
        raise ValueError(f"Instance appears infeasible inside window [{r}, {d}).")

    return merge_intervals(active + added), merge_intervals(added)


def compute_unbounded_preemptive_schedule(
    jobs: List[Job],
) -> Tuple[List[Tuple[int, int]], Dict[str, List[ScheduledPiece]], List[ActiveIteration]]:
    """
    Implements the greedy idea from Theorem 6 of the paper.

    The paper describes this as opening the latest interval required by the
    earliest-deadline work, then shrinking the opened interval and repeating.
    This implementation keeps the original time axis instead: for each job in
    nondecreasing deadline order, it measures how much active time already lies
    inside the job window and opens exactly the remaining deficit as late as
    possible. This is the same contraction argument represented without changing
    coordinates after each iteration.
    """

    active_intervals: List[Tuple[int, int]] = []
    iterations: List[ActiveIteration] = []

    for iteration, job in enumerate(sorted(jobs, key=lambda item: (item.d, item.r, item.id)), start=1):
        already_available = intersection_length(active_intervals, job.r, job.d)
        deficit = job.p - already_available
        added_intervals: List[Tuple[int, int]] = []

        if deficit > EPS:
            active_intervals, added_intervals = add_latest_inactive_time(
                active_intervals,
                job.r,
                job.d,
                deficit,
            )

        added_intervals_text = (
            "; ".join(f"[{start}, {end})" for start, end in added_intervals)
            if added_intervals
            else "none"
        )
        added_length = interval_length(added_intervals)
        iterations.append(
            ActiveIteration(
                iteration=iteration,
                deadline=job.d,
                added_intervals=added_intervals_text,
                added_length=added_length,
                considered_jobs=[job.id],
            )
        )

    schedule = assign_jobs_to_unbounded_schedule(jobs, active_intervals)
    validate_unbounded_schedule(jobs, schedule)

    return merge_intervals(active_intervals), schedule, iterations


def assign_jobs_to_unbounded_schedule(
    jobs: List[Job],
    active_intervals: List[Tuple[int, int]],
) -> Dict[str, List[ScheduledPiece]]:
    schedule: Dict[str, List[ScheduledPiece]] = {}

    for job in sorted(jobs, key=lambda item: (item.d, item.r, item.id)):
        remaining = job.p
        pieces: List[ScheduledPiece] = []
        possible_intervals = clip_intervals(active_intervals, job.r, job.d)

        for start, end in reversed(possible_intervals):
            if remaining <= EPS:
                break

            take = min(remaining, end - start)
            pieces.append(ScheduledPiece(job.id, end - take, end))
            remaining -= take

        if remaining > EPS:
            raise ValueError(f"Could not assign enough active time to job {job.id}.")

        schedule[job.id] = sorted(pieces, key=lambda piece: (piece.start, piece.end))

    return schedule


def build_interesting_intervals(
    unbounded_schedule: Dict[str, List[ScheduledPiece]]
) -> List[Tuple[int, int]]:
    points = set()

    for pieces in unbounded_schedule.values():
        for piece in pieces:
            points.add(piece.start)
            points.add(piece.end)

    sorted_points = sorted(points)
    return [
        (start, end)
        for start, end in zip(sorted_points, sorted_points[1:])
        if end > start + EPS
    ]


def jobs_running_on_interval(
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    start: int,
    end: int,
) -> List[str]:
    running_jobs = []

    for job_id, pieces in unbounded_schedule.items():
        if any(piece.start <= start + EPS and piece.end >= end - EPS for piece in pieces):
            running_jobs.append(job_id)

    return sorted(running_jobs)


def convert_to_bounded_preemptive_schedule(
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    g: int,
) -> List[MachinePiece]:
    """
    Implements the bounded-g conversion from Theorem 7.

    The unbounded schedule is partitioned into interesting intervals. In each
    interesting interval, the running jobs are assigned in arbitrary sorted order
    to ceil(n(I) / g) machines, filling each machine greedily up to capacity g.
    """

    if g <= 0:
        raise ValueError("Machine capacity g must be positive.")

    bounded_schedule: List[MachinePiece] = []
    interesting_intervals = build_interesting_intervals(unbounded_schedule)

    for interval_index, (start, end) in enumerate(interesting_intervals, start=1):
        running_jobs = jobs_running_on_interval(unbounded_schedule, start, end)
        number_of_machines = ceil(len(running_jobs) / g)

        for machine_index in range(number_of_machines):
            chunk = running_jobs[machine_index * g: (machine_index + 1) * g]
            if chunk:
                bounded_schedule.append(
                    MachinePiece(
                        machine_id=f"I{interval_index:03d}_M{machine_index + 1:03d}",
                        start=start,
                        end=end,
                        jobs=chunk,
                    )
                )

    validate_bounded_schedule(bounded_schedule, g)
    return bounded_schedule


def l2_preemptive_busy_time_schedule(
    jobs: List[Job],
    g: int,
) -> Tuple[
    List[Tuple[int, int]],
    Dict[str, List[ScheduledPiece]],
    List[ActiveIteration],
    List[MachinePiece],
]:
    active_intervals, unbounded_schedule, iterations = compute_unbounded_preemptive_schedule(
        jobs
    )
    bounded_schedule = convert_to_bounded_preemptive_schedule(unbounded_schedule, g)
    return active_intervals, unbounded_schedule, iterations, bounded_schedule


def total_unbounded_busy_time(active_intervals: List[Tuple[int, int]]) -> int:
    return interval_length(active_intervals)


def total_bounded_busy_time(bounded_schedule: List[MachinePiece]) -> int:
    return sum(piece.end - piece.start for piece in bounded_schedule)


def validate_unbounded_schedule(
    jobs: List[Job],
    schedule: Dict[str, List[ScheduledPiece]],
) -> None:
    jobs_by_id = {job.id: job for job in jobs}

    for job_id, pieces in schedule.items():
        job = jobs_by_id[job_id]
        total = sum(piece.end - piece.start for piece in pieces)

        if abs(total - job.p) > 1e-7:
            raise ValueError(
                f"Job {job_id} has {total} scheduled units but requires {job.p}."
            )

        previous_end = None
        for piece in sorted(pieces, key=lambda item: item.start):
            if piece.start < job.r - EPS or piece.end > job.d + EPS:
                raise ValueError(f"Piece for job {job_id} is outside its window.")
            if piece.end <= piece.start + EPS:
                raise ValueError(f"Piece for job {job_id} has non-positive length.")
            if previous_end is not None and piece.start < previous_end - EPS:
                raise ValueError(f"Job {job_id} has overlapping preemptive pieces.")
            previous_end = piece.end


def validate_bounded_schedule(bounded_schedule: List[MachinePiece], g: int) -> None:
    for piece in bounded_schedule:
        if len(piece.jobs) > g:
            raise ValueError(
                f"{piece.machine_id} exceeds capacity {g} with {len(piece.jobs)} jobs."
            )
        if piece.end <= piece.start + EPS:
            raise ValueError(f"{piece.machine_id} has non-positive length.")


def save_input_jobs_csv(output_file: str | Path, jobs: List[Job]) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Release", "Deadline", "processingTime"])
        for job in jobs:
            writer.writerow([job.id, job.r, job.d, job.p])


def save_active_iterations_csv(
    output_file: str | Path,
    iterations: List[ActiveIteration],
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "Iteration",
                "Current job deadline",
                "Added intervals",
                "Added length",
                "Job considered",
            ]
        )
        for item in iterations:
            writer.writerow(
                [
                    item.iteration,
                    item.deadline,
                    item.added_intervals,
                    item.added_length,
                    ", ".join(item.considered_jobs),
                ]
            )


def save_active_intervals_csv(
    output_file: str | Path,
    active_intervals: List[Tuple[int, int]],
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Interval", "Start", "End", "Length"])
        for index, (start, end) in enumerate(active_intervals, start=1):
            writer.writerow([f"Interval {index}", start, end, end - start])


def save_unbounded_schedule_csv(
    output_file: str | Path,
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Start", "End", "Length"])
        for job_id, pieces in sorted(unbounded_schedule.items()):
            for piece in pieces:
                writer.writerow([job_id, piece.start, piece.end, piece.end - piece.start])


def save_bounded_schedule_csv(
    output_file: str | Path,
    bounded_schedule: List[MachinePiece],
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Machine", "Start", "End", "Length", "Jobs on Machine"])
        for piece in bounded_schedule:
            writer.writerow(
                [
                    piece.machine_id,
                    piece.start,
                    piece.end,
                    piece.end - piece.start,
                    ", ".join(piece.jobs),
                ]
            )


def save_summary_csv(
    output_file: str | Path,
    g: int,
    active_intervals: List[Tuple[int, int]],
    bounded_schedule: List[MachinePiece],
) -> None:
    unbounded_busy_time = total_unbounded_busy_time(active_intervals)
    bounded_busy_time = total_bounded_busy_time(bounded_schedule)
    ratio = ""
    if unbounded_busy_time > EPS:
        ratio = bounded_busy_time / unbounded_busy_time

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Machine capacity g", g])
        writer.writerow(["Unbounded busy time", unbounded_busy_time])
        writer.writerow(["Bounded busy time", bounded_busy_time])
        writer.writerow(["Bounded / Unbounded ratio", ratio])


def save_all_results(
    jobs: List[Job],
    active_intervals: List[Tuple[int, int]],
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    iterations: List[ActiveIteration],
    bounded_schedule: List[MachinePiece],
    g: int,
    output_prefix: str,
    output_base_dir: str | Path = OUTPUTS_DIR,
) -> Dict[str, str]:
    output_dir = Path(output_base_dir) / output_prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    output_files = {
        "input_jobs": output_dir / f"{output_prefix}_input_jobs.csv",
        "active_iterations": output_dir / f"{output_prefix}_active_iterations.csv",
        "unbounded_active_intervals": output_dir
        / f"{output_prefix}_unbounded_active_intervals.csv",
        "unbounded_schedule": output_dir / f"{output_prefix}_unbounded_schedule.csv",
        "bounded_schedule": output_dir / f"{output_prefix}_bounded_schedule.csv",
        "summary": output_dir / f"{output_prefix}_summary.csv",
    }

    save_input_jobs_csv(output_files["input_jobs"], jobs)
    save_active_iterations_csv(output_files["active_iterations"], iterations)
    save_active_intervals_csv(output_files["unbounded_active_intervals"], active_intervals)
    save_unbounded_schedule_csv(output_files["unbounded_schedule"], unbounded_schedule)
    save_bounded_schedule_csv(output_files["bounded_schedule"], bounded_schedule)
    save_summary_csv(output_files["summary"], g, active_intervals, bounded_schedule)

    return {label: str(path) for label, path in output_files.items()}


def print_results(
    jobs: List[Job],
    g: int,
    active_intervals: List[Tuple[int, int]],
    unbounded_schedule: Dict[str, List[ScheduledPiece]],
    iterations: List[ActiveIteration],
    bounded_schedule: List[MachinePiece],
) -> None:
    print("L2 preemptive busy-time algorithm, Section 4.4 / Theorems 6 and 7")
    print("Machine capacity g:", g)
    print("Number of jobs:", len(jobs))

    print("\nGreedy active-interval iterations:")
    for item in iterations:
        print(
            f"{item.iteration}: deadline={item.deadline}, "
            f"added={item.added_intervals}, "
            f"job={item.considered_jobs}"
        )

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
            piece.jobs,
        )

    print("\nUnbounded busy time:", total_unbounded_busy_time(active_intervals))
    print("Bounded busy time:", total_bounded_busy_time(bounded_schedule))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the preemptive busy-time algorithm described in Section 4.4 "
            "of Chang, Khuller, and Mukherjee."
        )
    )
    parser.add_argument(
        "csv_file",
        nargs="?",
        default="jobs.csv",
        help="Input CSV filename. Relative paths are resolved from inputs/ first.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help=(
            "Output directory/file prefix. Defaults to "
            "<input_stem>_l2_preemptive."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_file = resolve_input_csv_path(args.csv_file)
    output_prefix = args.output_prefix or f"{csv_file.stem}_l2_preemptive"

    g, jobs = read_input_from_csv(csv_file)
    active_intervals, unbounded_schedule, iterations, bounded_schedule = (
        l2_preemptive_busy_time_schedule(jobs, g)
    )

    print_results(
        jobs,
        g,
        active_intervals,
        unbounded_schedule,
        iterations,
        bounded_schedule,
    )

    output_files = save_all_results(
        jobs,
        active_intervals,
        unbounded_schedule,
        iterations,
        bounded_schedule,
        g,
        output_prefix,
        OUTPUTS_DIR,
    )

    print("\nResults saved to separate CSV files:")
    for label, filename in output_files.items():
        print(f"{label}: {filename}")


if __name__ == "__main__":
    main()
