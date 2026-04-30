#!/usr/bin/env python3
"""
check_feasibility.py

Checks whether a bounded preemptive busy-time output schedule is feasible
for a given input CSV.

Usage examples on Windows:

    python check_feasibility.py inputs\input1.csv outputs\input1_l2_preemptive

or directly with the bounded schedule file:

    python check_feasibility.py inputs\input1.csv outputs\input1_l2_preemptive\input1_l2_preemptive_bounded_schedule.csv

Optional:

    python check_feasibility.py inputs\input1.csv outputs\input1_l2_preemptive --strict-summary

The checker validates:
  1. Input format and job feasibility.
  2. Output CSV format.
  3. Every scheduled job exists in the input.
  4. Every scheduled piece lies inside the job's release/deadline window.
  5. No job runs in parallel with itself.
  6. Each job receives exactly its required processing time.
  7. Each machine row has at most g jobs.
  8. Row Length equals End - Start.
  9. Sanity checks: nonnegative times, no duplicate jobs in one row,
     optional summary consistency, and suspicious unused/extra data.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


EPS = 1e-9


@dataclass(frozen=True)
class Job:
    job_id: str
    release: int
    deadline: int
    processing_time: int


@dataclass(frozen=True)
class ScheduleRow:
    row_number: int
    machine: str
    start: int
    end: int
    length: int
    jobs: List[str]


def parse_int(value: str, field_name: str, context: str = "") -> int:
    raw = value.strip()
    try:
        parsed = int(raw)
    except ValueError as exc:
        suffix = f" ({context})" if context else ""
        raise ValueError(f"{field_name}{suffix} must be an integer, got {value!r}.") from exc

    # Reject values like 2.0 or 02 if you want exact integer text.
    # This matches the style of the main algorithm script.
    if str(parsed) != raw:
        suffix = f" ({context})" if context else ""
        raise ValueError(f"{field_name}{suffix} must be an integer, got {value!r}.")

    return parsed


def read_input_csv(path: Path) -> Tuple[int, Dict[str, Job]]:
    with path.open(mode="r", newline="", encoding="utf-8-sig") as file:
        rows = [row for row in csv.reader(file, skipinitialspace=True) if row]

    if len(rows) < 4:
        raise ValueError("Input CSV must contain title, Capacity, capacity value, and job header rows.")

    if rows[1][0].strip() != "Capacity":
        raise ValueError("Input CSV row 2 must be exactly: Capacity")

    capacity = parse_int(rows[2][0], "Capacity")
    if capacity <= 0:
        raise ValueError("Capacity must be a positive integer.")

    header = [cell.strip() for cell in rows[3]]
    expected = ["Job", "Release", "Deadline", "processingTime"]
    if header != expected:
        raise ValueError(f"Input job header must be: {','.join(expected)}")

    jobs: Dict[str, Job] = {}

    for csv_row_number, row in enumerate(rows[4:], start=5):
        if len(row) < 4:
            continue

        job_id = row[0].strip()
        release = parse_int(row[1], "Release", f"row {csv_row_number}, job {job_id}")
        deadline = parse_int(row[2], "Deadline", f"row {csv_row_number}, job {job_id}")
        processing_time = parse_int(row[3], "processingTime", f"row {csv_row_number}, job {job_id}")

        if not job_id:
            raise ValueError(f"Input row {csv_row_number}: job name is empty.")
        if job_id in jobs:
            raise ValueError(f"Input row {csv_row_number}: duplicate job name {job_id!r}.")
        if deadline <= release:
            raise ValueError(f"Input row {csv_row_number}, job {job_id}: Deadline must be greater than Release.")
        if processing_time <= 0:
            raise ValueError(f"Input row {csv_row_number}, job {job_id}: processingTime must be positive.")
        if processing_time > deadline - release:
            raise ValueError(
                f"Input row {csv_row_number}, job {job_id}: processingTime is larger than Deadline - Release."
            )

        jobs[job_id] = Job(job_id, release, deadline, processing_time)

    if not jobs:
        raise ValueError("Input CSV contains no jobs.")

    return capacity, jobs


def split_jobs_cell(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def read_bounded_schedule_csv(path: Path) -> List[ScheduleRow]:
    with path.open(mode="r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file, skipinitialspace=True)
        if reader.fieldnames is None:
            raise ValueError("Bounded schedule CSV is empty.")

        normalized_fieldnames = [field.strip() for field in reader.fieldnames]
        expected = ["Machine", "Start", "End", "Length", "Jobs on Machine"]
        if normalized_fieldnames != expected:
            raise ValueError(
                "Bounded schedule header must be exactly: "
                "Machine,Start,End,Length,Jobs on Machine"
            )

        rows: List[ScheduleRow] = []
        for row_number, row in enumerate(reader, start=2):
            clean = {key.strip(): (value or "").strip() for key, value in row.items()}

            machine = clean["Machine"]
            start = parse_int(clean["Start"], "Start", f"output row {row_number}")
            end = parse_int(clean["End"], "End", f"output row {row_number}")
            length = parse_int(clean["Length"], "Length", f"output row {row_number}")
            jobs = split_jobs_cell(clean["Jobs on Machine"])

            rows.append(
                ScheduleRow(
                    row_number=row_number,
                    machine=machine,
                    start=start,
                    end=end,
                    length=length,
                    jobs=jobs,
                )
            )

    if not rows:
        raise ValueError("Bounded schedule CSV contains no schedule rows.")

    return rows


def read_summary_csv(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}

    with path.open(mode="r", newline="", encoding="utf-8-sig") as file:
        reader = csv.reader(file, skipinitialspace=True)
        header = next(reader, None)
        if header is None:
            return values

        for row in reader:
            if len(row) >= 2:
                values[row[0].strip()] = row[1].strip()

    return values


def find_bounded_schedule_file(path: Path) -> Path:
    if path.is_file():
        return path

    if not path.is_dir():
        raise FileNotFoundError(f"Output path does not exist: {path}")

    matches = sorted(path.glob("*_bounded_schedule.csv"))
    if not matches:
        raise FileNotFoundError(f"No *_bounded_schedule.csv file found in: {path}")
    if len(matches) > 1:
        names = "\n  ".join(str(item) for item in matches)
        raise ValueError(f"More than one bounded schedule CSV found. Please pass one directly:\n  {names}")

    return matches[0]


def find_summary_file(path: Path, bounded_file: Path) -> Optional[Path]:
    if path.is_file():
        candidates = sorted(path.parent.glob("*_summary.csv"))
    elif path.is_dir():
        candidates = sorted(path.glob("*_summary.csv"))
    else:
        candidates = []

    if len(candidates) == 1:
        return candidates[0]

    # Try same prefix as the bounded schedule.
    suffix = "_bounded_schedule.csv"
    if bounded_file.name.endswith(suffix):
        possible = bounded_file.with_name(bounded_file.name[: -len(suffix)] + "_summary.csv")
        if possible.is_file():
            return possible

    return None


def intervals_overlap(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    return min(a[1], b[1]) > max(a[0], b[0]) + EPS


def check_schedule(
    capacity: int,
    jobs: Dict[str, Job],
    schedule_rows: List[ScheduleRow],
    summary: Optional[Dict[str, str]] = None,
    strict_summary: bool = False,
) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    job_intervals: Dict[str, List[Tuple[int, int, int, str]]] = {job_id: [] for job_id in jobs}
    bounded_busy_time = 0

    seen_machine_intervals: Dict[str, List[Tuple[int, int, int]]] = {}

    for row in schedule_rows:
        row_context = f"output row {row.row_number}, machine {row.machine!r}"

        if not row.machine:
            errors.append(f"{row_context}: Machine is empty.")

        if row.start < 0 or row.end < 0:
            warnings.append(f"{row_context}: Start or End is negative. Check whether this is intended.")

        if row.end <= row.start:
            errors.append(f"{row_context}: End must be greater than Start.")

        actual_length = row.end - row.start
        if row.length != actual_length:
            errors.append(
                f"{row_context}: Length is {row.length}, but End - Start is {actual_length}."
            )

        if not row.jobs:
            warnings.append(f"{row_context}: no jobs listed.")
        if len(row.jobs) != len(set(row.jobs)):
            errors.append(f"{row_context}: duplicate job appears in Jobs on Machine.")

        if len(row.jobs) > capacity:
            errors.append(
                f"{row_context}: has {len(row.jobs)} jobs, exceeding capacity g={capacity}."
            )

        bounded_busy_time += actual_length

        previous_for_machine = seen_machine_intervals.setdefault(row.machine, [])
        for prev_start, prev_end, prev_row_number in previous_for_machine:
            if intervals_overlap((row.start, row.end), (prev_start, prev_end)):
                warnings.append(
                    f"{row_context}: overlaps another row with the same machine name "
                    f"from output row {prev_row_number}. If machine names are just labels, ignore this."
                )
        previous_for_machine.append((row.start, row.end, row.row_number))

        for job_id in row.jobs:
            if job_id not in jobs:
                errors.append(f"{row_context}: job {job_id!r} is not present in the input CSV.")
                continue

            job = jobs[job_id]
            if row.start < job.release or row.end > job.deadline:
                errors.append(
                    f"{row_context}: job {job_id!r} scheduled on [{row.start}, {row.end}) "
                    f"outside its window [{job.release}, {job.deadline})."
                )

            job_intervals[job_id].append((row.start, row.end, row.row_number, row.machine))

    for job_id, job in jobs.items():
        intervals = sorted(job_intervals[job_id], key=lambda item: (item[0], item[1], item[2]))
        total = sum(end - start for start, end, _, _ in intervals)

        if total != job.processing_time:
            errors.append(
                f"Job {job_id!r}: scheduled for {total} total time, "
                f"but processingTime is {job.processing_time}."
            )

        for first, second in zip(intervals, intervals[1:]):
            start_a, end_a, row_a, machine_a = first
            start_b, end_b, row_b, machine_b = second
            if intervals_overlap((start_a, end_a), (start_b, end_b)):
                errors.append(
                    f"Job {job_id!r}: runs in parallel/overlap on output rows "
                    f"{row_a} ({machine_a}, [{start_a}, {end_a})) and "
                    f"{row_b} ({machine_b}, [{start_b}, {end_b}))."
                )

    if summary is not None:
        summary_g = summary.get("Machine capacity g")
        if summary_g is not None and summary_g != str(capacity):
            message = f"Summary says Machine capacity g={summary_g}, but input has g={capacity}."
            if strict_summary:
                errors.append(message)
            else:
                warnings.append(message)

        summary_bounded = summary.get("Bounded busy time")
        if summary_bounded is not None:
            try:
                parsed_summary_bounded = int(summary_bounded)
                if parsed_summary_bounded != bounded_busy_time:
                    message = (
                        f"Summary says Bounded busy time={parsed_summary_bounded}, "
                        f"but bounded schedule rows sum to {bounded_busy_time}."
                    )
                    if strict_summary:
                        errors.append(message)
                    else:
                        warnings.append(message)
            except ValueError:
                message = f"Summary Bounded busy time is not an integer: {summary_bounded!r}."
                if strict_summary:
                    errors.append(message)
                else:
                    warnings.append(message)

    # Extra sanity checks that are not necessarily feasibility errors.
    all_output_jobs = {job_id for row in schedule_rows for job_id in row.jobs}
    missing_jobs = sorted(set(jobs) - all_output_jobs)
    if missing_jobs:
        # This will already be an error if their total processing time is not met,
        # but this warning makes it easy to spot.
        warnings.append("Jobs never mentioned in output: " + ", ".join(missing_jobs))

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether a bounded preemptive busy-time output schedule is feasible."
    )
    parser.add_argument("input_csv", help="Original input CSV, for example inputs\\input1.csv")
    parser.add_argument(
        "output",
        help=(
            "Either the output directory, for example outputs\\input1_l2_preemptive, "
            "or the *_bounded_schedule.csv file."
        ),
    )
    parser.add_argument(
        "--summary",
        default=None,
        help="Optional explicit *_summary.csv file. If omitted, the checker tries to find it automatically.",
    )
    parser.add_argument(
        "--strict-summary",
        action="store_true",
        help="Treat summary mismatches as errors instead of warnings.",
    )

    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    output_path = Path(args.output)

    try:
        bounded_file = find_bounded_schedule_file(output_path)
        summary_file = Path(args.summary) if args.summary else find_summary_file(output_path, bounded_file)

        capacity, jobs = read_input_csv(input_csv)
        schedule_rows = read_bounded_schedule_csv(bounded_file)
        summary = read_summary_csv(summary_file) if summary_file and summary_file.is_file() else None

        errors, warnings = check_schedule(
            capacity=capacity,
            jobs=jobs,
            schedule_rows=schedule_rows,
            summary=summary,
            strict_summary=args.strict_summary,
        )

        print("Feasibility checker")
        print("===================")
        print(f"Input CSV: {input_csv}")
        print(f"Bounded schedule CSV: {bounded_file}")
        print(f"Summary CSV: {summary_file if summary_file else 'not found / not checked'}")
        print(f"Machine capacity g: {capacity}")
        print(f"Jobs in input: {len(jobs)}")
        print(f"Schedule rows checked: {len(schedule_rows)}")

        if warnings:
            print("\nSanity warnings:")
            for warning in warnings:
                print(f"  WARNING: {warning}")

        if errors:
            print("\nFeasibility errors:")
            for error in errors:
                print(f"  ERROR: {error}")
            print("\nResult: NOT FEASIBLE")
            return 1

        print("\nResult: FEASIBLE")
        return 0

    except Exception as exc:
        print(f"Checker failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
