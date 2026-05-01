#!/usr/bin/env python3
# ============================================================
# Example usage:
#
# Generate one valid CSV file:
#   python generate_job_instances.py --num-jobs 50 --case-type low_overlap --capacity 2 --output-dir inputs
#
# Generate one file with a custom filename:
#   python generate_job_instances.py --num-jobs 50 --case-type low_overlap --capacity 2 --output-dir inputs --output-name my_custom_file.csv
#
# Generate a file with custom processing ranges and max deadline:
#   python generate_job_instances.py --num-jobs 99 --case-type low_overlap --capacity 2 \
#     --processing-min 50 --processing-max 150 --max-deadline 5000
#
# NOTE: 
# - `processing range` and `max deadline` can be explicitly controlled via CLI arguments.
# - `window length` and `max live jobs` are mathematically derived from the chosen `--case-type`. 
#   To change these drastically, pick a different case type (e.g. `loose_windows` vs `tight_windows`).
#
# Generate all 10 case-type CSV files and zip them:
#   python generate_job_instances.py --num-jobs 500 --all-cases --capacity 2 --output-dir inputs --zip
#
# Every generated CSV starts with these first 3 lines:
#   Busy Time
#   Capacity
#   2
#
# Then the job table begins:
#   Job,Release,Deadline,processingTime
# ============================================================

"""
generate_job_instances.py

Generate random but valid job-scheduling CSV instances.

CSV format produced:

Busy Time
Capacity
2
Job,Release,Deadline,processingTime
J1,10,55,26
J2,48,89,21
...

Supported case types:
  low_overlap
  high_overlap
  tight_windows
  loose_windows
  mixed_windows
  heavy_processing
  light_processing
  clustered_overlap
  nested_windows
  random_valid

Example:
  python generate_job_instances.py --num-jobs 50 --case-type high_overlap --capacity 2 --output-dir inputs

Generate all 10 cases:
  python generate_job_instances.py --num-jobs 50 --all-cases --capacity 2 --output-dir inputs --zip
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Sequence, Tuple, Union


JobRow = List[Union[int, str]]


@dataclass
class Config:
    num_jobs: int
    capacity: int
    processing_min: int
    processing_max: int
    max_deadline: int
    seed: int
    output_dir: Path


CASE_INFO: Dict[str, Tuple[str, str]] = {
    "low_overlap": ("C1", "low_overlap_easy"),
    "high_overlap": ("C2", "high_overlap_large_gap"),
    "tight_windows": ("C3", "tight_windows"),
    "loose_windows": ("C4", "loose_windows"),
    "mixed_windows": ("C5", "mixed_windows"),
    "heavy_processing": ("C6", "heavy_processing"),
    "light_processing": ("C7", "light_processing"),
    "clustered_overlap": ("C8", "clustered_overlap"),
    "nested_windows": ("C9", "nested_windows"),
    "random_valid": ("C10", "random_valid"),
}


def processing_value(index: int, low: int, high: int, offset: int = 0) -> int:
    """Deterministic pseudo-random-looking processing value in [low, high]."""
    return low + ((37 * index + 11 + offset) % (high - low + 1))


def random_processing(rng: random.Random, cfg: Config, low: int | None = None, high: int | None = None) -> int:
    """Random processing time within the requested range."""
    p_low = cfg.processing_min if low is None else max(cfg.processing_min, low)
    p_high = cfg.processing_max if high is None else min(cfg.processing_max, high)

    if p_low > p_high:
        p_low = cfg.processing_min
        p_high = cfg.processing_max

    return rng.randint(p_low, p_high)


def add_job(jobs: List[JobRow], used_windows: set[Tuple[int, int]], release: int, deadline: int, processing: int) -> bool:
    """Try to add one job if it is valid and has a unique release-deadline pair."""
    if deadline <= release:
        return False
    if processing <= 0:
        return False
    if processing > deadline - release:
        return False
    if (release, deadline) in used_windows:
        return False

    job_id = f"J{len(jobs) + 1}"
    jobs.append([job_id, release, deadline, processing])
    used_windows.add((release, deadline))
    return True


def validate_jobs(jobs: Sequence[JobRow], cfg: Config) -> None:
    """Strict validation run before writing any CSV."""
    if len(jobs) != cfg.num_jobs:
        raise ValueError(f"Expected {cfg.num_jobs} jobs, got {len(jobs)}.")

    job_ids = [str(row[0]) for row in jobs]
    windows = [(int(row[1]), int(row[2])) for row in jobs]
    triples = [(int(row[1]), int(row[2]), int(row[3])) for row in jobs]

    if len(set(job_ids)) != len(job_ids):
        raise ValueError("Duplicate Job IDs found.")

    if len(set(windows)) != len(windows):
        raise ValueError("Duplicate (Release, Deadline) pairs found.")

    if len(set(triples)) != len(triples):
        raise ValueError("Duplicate (Release, Deadline, processingTime) triples found.")

    for job, release, deadline, processing in jobs:
        release = int(release)
        deadline = int(deadline)
        processing = int(processing)

        if deadline <= release:
            raise ValueError(f"{job}: Deadline must be greater than Release.")

        if not (cfg.processing_min <= processing <= cfg.processing_max):
            raise ValueError(f"{job}: processingTime {processing} is outside the allowed range.")

        if processing > deadline - release:
            raise ValueError(
                f"{job}: processingTime {processing} exceeds window length {deadline - release}."
            )

        if deadline > cfg.max_deadline:
            raise ValueError(f"{job}: Deadline {deadline} exceeds max deadline {cfg.max_deadline}.")


def write_job_csv(path: Path, jobs: Sequence[JobRow], cfg: Config) -> None:
    """
    Write the special CSV format requested by the project.

    First 3 lines:
      Busy Time
      Capacity
      <capacity>

    Then the actual job table header.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Busy Time"])
        writer.writerow(["Capacity"])
        writer.writerow([cfg.capacity])
        writer.writerow(["Job", "Release", "Deadline", "processingTime"])
        writer.writerows(jobs)


def generate_random_valid_jobs(
    cfg: Config,
    rng: random.Random,
    slack_min: int,
    slack_max: int,
    p_low: int | None = None,
    p_high: int | None = None,
    release_min: int = 1,
    release_max: int | None = None,
) -> List[JobRow]:
    """Generic generate-validate-retry random generator."""
    jobs: List[JobRow] = []
    used_windows: set[Tuple[int, int]] = set()

    release_upper = release_max if release_max is not None else cfg.max_deadline - cfg.processing_max - slack_max - 1
    release_upper = max(release_min, release_upper)

    max_attempts = cfg.num_jobs * 10000
    attempts = 0

    while len(jobs) < cfg.num_jobs and attempts < max_attempts:
        attempts += 1

        processing = random_processing(rng, cfg, p_low, p_high)
        release = rng.randint(release_min, release_upper)

        max_possible_slack = cfg.max_deadline - release - processing
        if max_possible_slack < slack_min:
            continue

        actual_slack_max = min(slack_max, max_possible_slack)
        slack = rng.randint(slack_min, actual_slack_max)
        deadline = release + processing + slack

        add_job(jobs, used_windows, release, deadline, processing)

    if len(jobs) < cfg.num_jobs:
        raise RuntimeError(
            f"Could not generate {cfg.num_jobs} valid random jobs. "
            "Try increasing max_deadline or loosening constraints."
        )

    return jobs


def overlap_pairs(
    cfg: Config,
    count: int,
    rng: random.Random,
    common_start: int | None = None,
    common_end: int | None = None,
) -> List[Tuple[int, int]]:
    """
    Create unique windows that all contain a common block.

    Every generated pair satisfies:
      release <= common_start
      deadline >= common_end
      common_end - common_start >= processing_max
    """
    if common_start is None or common_end is None:
        d_count = int(math.ceil(math.sqrt(count))) + 5
        r_count = int(math.ceil(count / d_count)) + 5

        common_end = cfg.max_deadline - d_count + 1
        common_start = common_end - cfg.processing_max

        if common_start < 1:
            raise ValueError("max_deadline is too small for the requested processing range.")

        release_min = max(1, common_start - r_count + 1)
        release_values = list(range(release_min, common_start + 1))
        deadline_values = list(range(common_end, cfg.max_deadline + 1))
    else:
        if common_end - common_start < cfg.processing_max:
            raise ValueError("Common overlap block must be at least processing_max long.")

        d_count = int(math.ceil(math.sqrt(count))) + 8
        r_count = int(math.ceil(count / d_count)) + 8

        release_values = list(range(max(1, common_start - r_count + 1), common_start + 1))
        deadline_values = list(range(common_end, min(cfg.max_deadline, common_end + d_count - 1) + 1))

    pairs = [(r, d) for r in release_values for d in deadline_values]

    if len(pairs) < count:
        raise RuntimeError(
            f"Not enough unique overlap windows. Need {count}, have {len(pairs)}."
        )

    rng.shuffle(pairs)
    return pairs[:count]


def generate_low_overlap(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Jobs are spread across the full time horizon with relatively short windows."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    # Use smaller processing values if allowed, making this a relatively easy case.
    p_high = min(cfg.processing_max, max(cfg.processing_min, 40))
    timeline_span = max(1, cfg.max_deadline - cfg.processing_max - 60)

    for i in range(1, cfg.num_jobs + 1):
        processing = processing_value(i, cfg.processing_min, p_high)
        release = 1 + round((i - 1) * timeline_span / max(1, cfg.num_jobs - 1))
        window = processing + 2 + (i % 7)
        deadline = release + window

        while deadline > cfg.max_deadline or (release, deadline) in used:
            release = max(1, release - 1)
            deadline = release + window
            window += 1

        add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_high_overlap(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Many jobs share one common block; bounded and unbounded busy times differ strongly."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()
    pairs = overlap_pairs(cfg, cfg.num_jobs, rng)

    for i, (release, deadline) in enumerate(pairs, start=1):
        processing = cfg.processing_max if i == 1 else random_processing(rng, cfg)
        add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_tight_windows(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Each window is very close to processingTime: deadline = release + p + small slack."""
    return generate_random_valid_jobs(
        cfg,
        rng,
        slack_min=0,
        slack_max=5,
        release_min=1,
        release_max=cfg.max_deadline - cfg.processing_max - 6,
    )


def generate_loose_windows(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Large windows give many scheduling choices."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    max_attempts = cfg.num_jobs * 10000
    attempts = 0

    while len(jobs) < cfg.num_jobs and attempts < max_attempts:
        attempts += 1
        processing = random_processing(rng, cfg)
        release = rng.randint(1, max(1, cfg.max_deadline // 2))
        deadline = rng.randint(max(cfg.max_deadline - 500, release + processing), cfg.max_deadline)
        add_job(jobs, used, release, deadline, processing)

    if len(jobs) < cfg.num_jobs:
        raise RuntimeError("Could not generate enough loose-window jobs.")

    return jobs


def generate_mixed_windows(cfg: Config, rng: random.Random) -> List[JobRow]:
    """A realistic mixture: tight, loose, high-overlap, clustered, and random jobs."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    part = cfg.num_jobs // 5

    # 1. Tight jobs
    while len(jobs) < part:
        processing = random_processing(rng, cfg)
        release = rng.randint(1, max(1, cfg.max_deadline - cfg.processing_max - 10))
        deadline = release + processing + rng.randint(0, 6)
        add_job(jobs, used, release, deadline, processing)

    # 2. Loose jobs
    target = 2 * part
    while len(jobs) < target:
        processing = random_processing(rng, cfg)
        release = rng.randint(1, max(1, cfg.max_deadline // 2))
        deadline = rng.randint(max(release + processing, cfg.max_deadline - 500), cfg.max_deadline)
        add_job(jobs, used, release, deadline, processing)

    # 3. High-overlap jobs
    target = 3 * part
    pairs = overlap_pairs(cfg, max(1, target - len(jobs)), rng)
    for release, deadline in pairs:
        if len(jobs) >= target:
            break
        processing = cfg.processing_max if len(jobs) == 2 * part else random_processing(rng, cfg)
        add_job(jobs, used, release, deadline, processing)

    # 4. Clustered jobs
    target = 4 * part
    cluster_blocks = make_cluster_blocks(cfg, 4)
    cluster_index = 0
    while len(jobs) < target:
        start, end = cluster_blocks[cluster_index % len(cluster_blocks)]
        processing = random_processing(rng, cfg)
        release = rng.randint(max(1, start - 50), start)
        deadline = rng.randint(end, min(cfg.max_deadline, end + 60))
        add_job(jobs, used, release, deadline, processing)
        cluster_index += 1

    # 5. Random valid jobs
    while len(jobs) < cfg.num_jobs:
        processing = random_processing(rng, cfg)
        release = rng.randint(1, max(1, cfg.max_deadline - cfg.processing_max - 10))
        max_slack = cfg.max_deadline - release - processing
        if max_slack < 0:
            continue
        slack = rng.randint(0, min(180, max_slack))
        deadline = release + processing + slack
        add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_heavy_processing(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Mostly high processing times, usually close to 80-100."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()
    pairs = overlap_pairs(cfg, cfg.num_jobs, rng)

    heavy_low = min(max(cfg.processing_min, 80), cfg.processing_max)
    heavy_high = cfg.processing_max

    for i, (release, deadline) in enumerate(pairs, start=1):
        processing = random_processing(rng, cfg, heavy_low, heavy_high)
        add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_light_processing(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Smaller processing times, typically 20-40."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    light_high = min(cfg.processing_max, max(cfg.processing_min, 40))
    timeline_span = max(1, cfg.max_deadline - light_high - 80)

    for i in range(1, cfg.num_jobs + 1):
        processing = processing_value(i, cfg.processing_min, light_high)
        release = 1 + round((i - 1) * timeline_span / max(1, cfg.num_jobs - 1))
        window = processing + 8 + (i % 18)
        deadline = release + window

        while deadline > cfg.max_deadline or (release, deadline) in used:
            release = max(1, release - 1)
            deadline = release + window
            window += 1

        add_job(jobs, used, release, deadline, processing)

    return jobs


def make_cluster_blocks(cfg: Config, num_clusters: int = 5) -> List[Tuple[int, int]]:
    """Create common overlap blocks for clustered jobs."""
    block_len = cfg.processing_max
    usable_start = 100
    usable_end = cfg.max_deadline - block_len - 80

    if usable_end <= usable_start:
        raise ValueError("max_deadline is too small for clustered instances.")

    if num_clusters == 1:
        starts = [usable_start]
    else:
        starts = [
            round(usable_start + i * (usable_end - usable_start) / (num_clusters - 1))
            for i in range(num_clusters)
        ]

    return [(s, s + block_len) for s in starts]


def generate_clustered_overlap(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Jobs are divided into 5 overlapping clusters."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    num_clusters = 5
    base = cfg.num_jobs // num_clusters
    remainder = cfg.num_jobs % num_clusters
    cluster_blocks = make_cluster_blocks(cfg, num_clusters)

    for c, (start, end) in enumerate(cluster_blocks):
        count = base + (1 if c < remainder else 0)
        pairs = overlap_pairs(cfg, count, random.Random(cfg.seed + 100 + c), start, end)

        for k, (release, deadline) in enumerate(pairs):
            processing = cfg.processing_max if k == 0 else random_processing(rng, cfg)
            add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_nested_windows(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Some jobs have smaller windows nested inside larger windows."""
    jobs: List[JobRow] = []
    used: set[Tuple[int, int]] = set()

    num_groups = 5
    base = cfg.num_jobs // num_groups
    remainder = cfg.num_jobs % num_groups

    group_spacing = max(1, (cfg.max_deadline - 250) // num_groups)
    outer_len = max(cfg.processing_max + base + 40, 180)

    for g in range(num_groups):
        count = base + (1 if g < remainder else 0)
        base_release = 30 + g * group_spacing
        base_deadline = min(cfg.max_deadline, base_release + outer_len)

        for k in range(count):
            processing = random_processing(rng, cfg)
            release = base_release + k // 2
            deadline = base_deadline - k // 2

            if deadline - release < processing:
                deadline = release + processing + 1 + (k % 9)

            if deadline > cfg.max_deadline:
                deadline = cfg.max_deadline
                release = deadline - processing - 1 - (k % 9)

            while (release, deadline) in used or processing > deadline - release or deadline > cfg.max_deadline:
                if deadline < cfg.max_deadline:
                    deadline += 1
                elif release > 1:
                    release -= 1
                else:
                    raise RuntimeError("Could not repair nested-window duplicate.")

            add_job(jobs, used, release, deadline, processing)

    return jobs


def generate_random_valid(cfg: Config, rng: random.Random) -> List[JobRow]:
    """Completely random, but still valid."""
    return generate_random_valid_jobs(
        cfg,
        rng,
        slack_min=0,
        slack_max=180,
        release_min=1,
        release_max=cfg.max_deadline - cfg.processing_max - 1,
    )


GENERATORS: Dict[str, Callable[[Config, random.Random], List[JobRow]]] = {
    "low_overlap": generate_low_overlap,
    "high_overlap": generate_high_overlap,
    "tight_windows": generate_tight_windows,
    "loose_windows": generate_loose_windows,
    "mixed_windows": generate_mixed_windows,
    "heavy_processing": generate_heavy_processing,
    "light_processing": generate_light_processing,
    "clustered_overlap": generate_clustered_overlap,
    "nested_windows": generate_nested_windows,
    "random_valid": generate_random_valid,
}


def max_live_jobs(jobs: Sequence[JobRow]) -> int:
    """Maximum number of jobs whose windows overlap at any integer time."""
    min_time = min(int(row[1]) for row in jobs)
    max_time = max(int(row[2]) for row in jobs)

    max_live = 0
    for t in range(min_time, max_time):
        live = sum(1 for _, r, d, _ in jobs if int(r) <= t < int(d))
        max_live = max(max_live, live)

    return max_live


def theorem7_style_metrics(jobs: Sequence[JobRow], capacity: int) -> Tuple[int, int, float]:
    """
    Quick metrics for the Theorem 7-style preemptive busy-time schedule.

    This is only for reporting/debugging the instance. It does not change the CSV.
    """
    active_slots: set[int] = set()

    for job, release, deadline, processing in sorted(
        jobs, key=lambda row: (int(row[2]), int(row[1]), str(row[0]))
    ):
        release = int(release)
        deadline = int(deadline)
        processing = int(processing)

        already = sum(1 for t in active_slots if release <= t < deadline)
        need = processing - already

        if need > 0:
            candidates = [
                t for t in range(deadline - 1, release - 1, -1)
                if t not in active_slots
            ]

            if len(candidates) < need:
                raise RuntimeError(f"Could not find enough active slots for {job}.")

            active_slots.update(candidates[:need])

    slot_load: Dict[int, int] = defaultdict(int)
    sorted_active = sorted(active_slots, reverse=True)

    for job, release, deadline, processing in sorted(
        jobs, key=lambda row: (int(row[2]), int(row[1]), str(row[0]))
    ):
        release = int(release)
        deadline = int(deadline)
        processing = int(processing)

        feasible = [t for t in sorted_active if release <= t < deadline]

        if len(feasible) < processing:
            raise RuntimeError(f"Internal error: insufficient active slots for {job}.")

        for t in feasible[:processing]:
            slot_load[t] += 1

    unbounded_busy_time = len(slot_load)
    bounded_busy_time = sum(math.ceil(load / capacity) for load in slot_load.values())
    ratio = bounded_busy_time / unbounded_busy_time if unbounded_busy_time else 0.0

    return unbounded_busy_time, bounded_busy_time, ratio


def instance_filename(cfg: Config, case_type: str) -> str:
    case_id, case_name = CASE_INFO[case_type]
    return f"{cfg.num_jobs}_jobs_{case_id}_{case_name}.csv"


def normalize_output_name(output_name: str) -> str:
    """
    Normalize a custom output filename.

    Examples:
      my_file      -> my_file.csv
      my_file.csv  -> my_file.csv
    """
    filename = output_name.strip()

    if not filename:
        raise ValueError("--output-name cannot be empty.")

    if "/" in filename or "\\" in filename:
        raise ValueError("--output-name should be a filename only, not a path. Use --output-dir for folders.")

    if not filename.lower().endswith(".csv"):
        filename += ".csv"

    return filename


def generate_one_case(cfg: Config, case_type: str, output_name: str | None = None) -> Path:
    rng = random.Random(cfg.seed + list(CASE_INFO.keys()).index(case_type) * 997)
    jobs = GENERATORS[case_type](cfg, rng)

    validate_jobs(jobs, cfg)

    if output_name:
        filename = normalize_output_name(output_name)
    else:
        filename = instance_filename(cfg, case_type)

    output_path = cfg.output_dir / filename
    write_job_csv(output_path, jobs, cfg)

    unbounded, bounded, ratio = theorem7_style_metrics(jobs, cfg.capacity)
    windows = [int(d) - int(r) for _, r, d, _ in jobs]
    processing_values = [int(p) for _, _, _, p in jobs]

    print(f"\nCreated: {output_path}")
    print(f"  jobs: {len(jobs)}")
    print(f"  capacity: {cfg.capacity}")
    print(f"  max deadline: {max(int(row[2]) for row in jobs)}")
    print(f"  processing range: {min(processing_values)}-{max(processing_values)}")
    print(f"  window length range: {min(windows)}-{max(windows)}")
    print(f"  max live jobs: {max_live_jobs(jobs)}")
    print(f"  theorem7-style unbounded busy time: {unbounded}")
    print(f"  theorem7-style bounded busy time: {bounded}")
    print(f"  bounded/unbounded ratio: {ratio:.2f}x")

    return output_path


def make_zip(output_dir: Path, files: Sequence[Path], zip_name: str) -> Path:
    zip_path = output_dir / zip_name

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for file_path in files:
            z.write(file_path, arcname=file_path.name)

    return zip_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate valid random job-scheduling CSV instances."
    )

    parser.add_argument("--num-jobs", type=int, default=50, help="Number of jobs to generate.")
    parser.add_argument(
        "--case-type",
        choices=list(CASE_INFO.keys()),
        help="Case type to generate. Not needed when --all-cases is used.",
    )
    parser.add_argument("--all-cases", action="store_true", help="Generate all 10 case types.")
    parser.add_argument("--capacity", type=int, default=2, help="Capacity value written in the CSV metadata.")
    parser.add_argument("--processing-min", type=int, default=20, help="Minimum processing time.")
    parser.add_argument("--processing-max", type=int, default=100, help="Maximum processing time.")
    parser.add_argument("--max-deadline", type=int, default=2000, help="Maximum allowed deadline.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible instances.")
    parser.add_argument("--output-dir", default="inputs", help="Directory where CSV files will be written.")
    parser.add_argument(
        "--output-name",
        help=(
            "Custom CSV filename for single-case generation. "
            "Example: my_instance.csv. This cannot be used with --all-cases."
        ),
    )
    parser.add_argument("--zip", action="store_true", help="Zip generated CSV files.")

    return parser.parse_args()


def main() -> None:
    start_time = time.perf_counter()
    args = parse_args()

    if args.num_jobs <= 0:
        raise ValueError("--num-jobs must be positive.")

    if args.capacity <= 0:
        raise ValueError("--capacity must be positive.")

    if args.processing_min <= 0 or args.processing_max < args.processing_min:
        raise ValueError("Invalid processing range.")

    if args.max_deadline <= args.processing_max + 10:
        raise ValueError("--max-deadline must be comfortably larger than --processing-max.")

    if not args.all_cases and not args.case_type:
        raise ValueError("Please provide --case-type or use --all-cases.")

    if args.all_cases and args.output_name:
        raise ValueError("--output-name can only be used when generating one case. Do not use it with --all-cases.")

    cfg = Config(
        num_jobs=args.num_jobs,
        capacity=args.capacity,
        processing_min=args.processing_min,
        processing_max=args.processing_max,
        max_deadline=args.max_deadline,
        seed=args.seed,
        output_dir=Path(args.output_dir),
    )

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    if args.all_cases:
        selected_cases = list(CASE_INFO.keys())
    else:
        selected_cases = [args.case_type]

    generated_files = [
        generate_one_case(
            cfg,
            case_type,
            output_name=args.output_name if not args.all_cases else None,
        )
        for case_type in selected_cases
    ]

    readme_path = cfg.output_dir / "README_generated_instances.txt"
    readme_path.write_text(
        "Generated job scheduling instances.\n"
        "Each CSV has the first three metadata lines:\n"
        "Busy Time\n"
        "Capacity\n"
        f"{cfg.capacity}\n"
        "Then the job table header:\n"
        "Job,Release,Deadline,processingTime\n"
    )

    if args.zip:
        zip_path = make_zip(
            cfg.output_dir,
            generated_files + [readme_path],
            f"{cfg.num_jobs}_jobs_generated_cases.zip",
        )
        print(f"\nZIP created: {zip_path}")

    elapsed = time.perf_counter() - start_time
    print(f"\nTotal runtime: {elapsed:.6f} seconds")


if __name__ == "__main__":
    main()
