import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import pulp
except ImportError as exc:
    raise SystemExit(
        "This script needs PuLP. Install it with: python -m pip install pulp"
    ) from exc

from l2_preemptive_busy_time import (
    INPUTS_DIR,
    OUTPUTS_DIR,
    Job,
    MachinePiece,
    read_input_from_csv,
    resolve_input_csv_path,
    save_bounded_schedule_csv,
    save_input_jobs_csv,
    validate_bounded_schedule,
)


EPS = 1e-7


@dataclass(frozen=True)
class IlpResult:
    status: str
    objective_value: int | None
    machine_count: int
    bounded_schedule: List[MachinePiece]


def build_time_indexed_model(
    jobs: List[Job],
    g: int,
    machine_count: int,
    add_symmetry_breaking: bool = True,
) -> Tuple[pulp.LpProblem, Dict[Tuple[str, int, int], pulp.LpVariable], Dict[Tuple[int, int], pulp.LpVariable], List[int]]:
    if g <= 0:
        raise ValueError("Machine capacity g must be positive.")
    if not jobs:
        raise ValueError("The input instance contains no jobs.")
    if machine_count <= 0:
        raise ValueError("The number of candidate machines must be positive.")
    ensure_unique_job_ids(jobs)

    start_time = min(job.r for job in jobs)
    end_time = max(job.d for job in jobs)
    time_slots = list(range(start_time, end_time))
    machines = list(range(machine_count))

    model = pulp.LpProblem("preemptive_busy_time", pulp.LpMinimize)

    y = {
        (machine, time): pulp.LpVariable(
            f"y_m{machine}_t{time}",
            lowBound=0,
            upBound=1,
            cat=pulp.LpBinary,
        )
        for machine in machines
        for time in time_slots
    }

    x = {
        (job.id, machine, time): pulp.LpVariable(
            f"x_j{job_index}_{clean_name(job.id)}_m{machine}_t{time}",
            lowBound=0,
            upBound=1,
            cat=pulp.LpBinary,
        )
        for job_index, job in enumerate(jobs)
        for machine in machines
        for time in range(job.r, job.d)
    }

    model += pulp.lpSum(y[machine, time] for machine in machines for time in time_slots)

    for job_index, job in enumerate(jobs):
        model += (
            pulp.lpSum(
                x[job.id, machine, time]
                for machine in machines
                for time in range(job.r, job.d)
            )
            == job.p,
            f"processing_j{job_index}",
        )

    for job_index, job in enumerate(jobs):
        for time in range(job.r, job.d):
            model += (
                pulp.lpSum(x[job.id, machine, time] for machine in machines) <= 1,
                f"no_parallel_j{job_index}_t{time}",
            )

    jobs_by_time = {
        time: [job for job in jobs if job.r <= time < job.d]
        for time in time_slots
    }
    for machine in machines:
        for time in time_slots:
            model += (
                pulp.lpSum(
                    x[job.id, machine, time]
                    for job in jobs_by_time[time]
                )
                <= g * y[machine, time],
                f"capacity_m{machine}_t{time}",
            )

    if add_symmetry_breaking:
        for time in time_slots:
            for machine in range(machine_count - 1):
                model += (
                    y[machine, time] >= y[machine + 1, time],
                    f"machine_order_m{machine}_t{time}",
                )

    return model, x, y, time_slots


def ensure_unique_job_ids(jobs: List[Job]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for job in jobs:
        if job.id in seen:
            duplicates.add(job.id)
        seen.add(job.id)
    if duplicates:
        raise ValueError("Duplicate job IDs are not supported: " + ", ".join(sorted(duplicates)))


def clean_name(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value)


def make_solver(args: argparse.Namespace) -> pulp.LpSolver:
    common_options = {
        "msg": not args.quiet,
        "timeLimit": args.time_limit,
    }

    if args.solver == "gurobi-api":
        solver = pulp.GUROBI(**common_options)
    elif args.solver == "gurobi-cmd":
        solver = pulp.GUROBI_CMD(path=args.gurobi_cmd, **common_options)
    elif args.solver == "gurobi":
        api_solver = pulp.GUROBI(**common_options)
        if api_solver.available():
            return api_solver
        solver = pulp.GUROBI_CMD(path=args.gurobi_cmd, **common_options)
    elif args.solver == "cbc":
        solver = pulp.PULP_CBC_CMD(**common_options)
    else:
        raise ValueError(f"Unsupported solver: {args.solver}")

    if not solver.available():
        raise RuntimeError(
            f"PuLP cannot find an available {args.solver} solver. "
            "Use --solver cbc for PuLP's bundled CBC solver, or make sure "
            "Gurobi/gurobi_cl is installed and licensed."
        )

    return solver


def solve_busy_time_ilp(
    jobs: List[Job],
    g: int,
    solver: pulp.LpSolver,
    machine_count: int | None = None,
    add_symmetry_breaking: bool = True,
) -> IlpResult:
    candidate_machine_count = machine_count or len(jobs)
    model, x, _y, time_slots = build_time_indexed_model(
        jobs=jobs,
        g=g,
        machine_count=candidate_machine_count,
        add_symmetry_breaking=add_symmetry_breaking,
    )

    model.solve(solver)
    status = pulp.LpStatus[model.status]

    objective_value = None
    bounded_schedule: List[MachinePiece] = []
    if status in {"Optimal", "Feasible"}:
        raw_objective = pulp.value(model.objective)
        if raw_objective is not None:
            objective_value = int(round(raw_objective))
        bounded_schedule = extract_bounded_schedule(
            jobs=jobs,
            x=x,
            machine_count=candidate_machine_count,
            time_slots=time_slots,
        )
        validate_bounded_schedule(bounded_schedule, g)

    return IlpResult(
        status=status,
        objective_value=objective_value,
        machine_count=candidate_machine_count,
        bounded_schedule=bounded_schedule,
    )


def extract_bounded_schedule(
    jobs: List[Job],
    x: Dict[Tuple[str, int, int], pulp.LpVariable],
    machine_count: int,
    time_slots: List[int],
) -> List[MachinePiece]:
    rows: List[MachinePiece] = []
    job_ids = sorted(job.id for job in jobs)

    for machine in range(machine_count):
        current_start: int | None = None
        current_end: int | None = None
        current_jobs: Tuple[str, ...] = ()

        for time in time_slots:
            running_jobs = tuple(
                job_id
                for job_id in job_ids
                if pulp.value(x.get((job_id, machine, time), 0)) is not None
                and pulp.value(x.get((job_id, machine, time), 0)) > 0.5
            )

            if running_jobs and running_jobs == current_jobs and current_end == time:
                current_end = time + 1
                continue

            if current_jobs and current_start is not None and current_end is not None:
                rows.append(
                    MachinePiece(
                        machine_id=f"M{machine + 1:03d}",
                        start=current_start,
                        end=current_end,
                        jobs=list(current_jobs),
                    )
                )

            if running_jobs:
                current_start = time
                current_end = time + 1
                current_jobs = running_jobs
            else:
                current_start = None
                current_end = None
                current_jobs = ()

        if current_jobs and current_start is not None and current_end is not None:
            rows.append(
                MachinePiece(
                    machine_id=f"M{machine + 1:03d}",
                    start=current_start,
                    end=current_end,
                    jobs=list(current_jobs),
                )
            )

    return rows


def save_job_schedule_csv(
    output_file: str | Path,
    bounded_schedule: List[MachinePiece],
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Job", "Machine", "Start", "End", "Length"])
        for piece in bounded_schedule:
            for job_id in piece.jobs:
                writer.writerow(
                    [
                        job_id,
                        piece.machine_id,
                        piece.start,
                        piece.end,
                        piece.end - piece.start,
                    ]
                )


def save_summary_csv(
    output_file: str | Path,
    g: int,
    result: IlpResult,
    solver_name: str,
) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Solver", solver_name])
        writer.writerow(["Status", result.status])
        writer.writerow(["Machine capacity g", g])
        writer.writerow(["Candidate machines", result.machine_count])
        writer.writerow(["Bounded busy time", result.objective_value or ""])


def save_all_results(
    jobs: List[Job],
    g: int,
    result: IlpResult,
    solver_name: str,
    output_prefix: str,
    output_base_dir: str | Path = OUTPUTS_DIR,
) -> Dict[str, str]:
    output_dir = Path(output_base_dir) / output_prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    output_files = {
        "input_jobs": output_dir / f"{output_prefix}_input_jobs.csv",
        "bounded_schedule": output_dir / f"{output_prefix}_bounded_schedule.csv",
        "job_schedule": output_dir / f"{output_prefix}_job_schedule.csv",
        "summary": output_dir / f"{output_prefix}_summary.csv",
    }

    save_input_jobs_csv(output_files["input_jobs"], jobs)
    save_bounded_schedule_csv(output_files["bounded_schedule"], result.bounded_schedule)
    save_job_schedule_csv(output_files["job_schedule"], result.bounded_schedule)
    save_summary_csv(output_files["summary"], g, result, solver_name)

    return {label: str(path) for label, path in output_files.items()}


def print_results(jobs: List[Job], g: int, result: IlpResult) -> None:
    print("Preemptive busy-time ILP with PuLP")
    print("Machine capacity g:", g)
    print("Number of jobs:", len(jobs))
    print("Candidate machines:", result.machine_count)
    print("Solver status:", result.status)
    print("Optimal bounded busy time:", result.objective_value)

    print("\nBounded-g machine schedule:")
    for piece in result.bounded_schedule:
        print(
            piece.machine_id,
            f"[{piece.start}, {piece.end})",
            "jobs:",
            piece.jobs,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Solve the preemptive busy-time problem as a time-indexed ILP with "
            "PuLP and Gurobi."
        )
    )
    parser.add_argument(
        "csv_file",
        nargs="?",
        default="jobs.csv",
        help=f"Input CSV filename. Relative paths are resolved from {INPUTS_DIR}/ first.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Output directory/file prefix. Defaults to <input_stem>_ilp_busy_time.",
    )
    parser.add_argument(
        "--solver",
        choices=["gurobi", "gurobi-api", "gurobi-cmd", "cbc"],
        default="gurobi",
        help=(
            "Solver used by PuLP. 'gurobi' tries the Gurobi Python API first "
            "and then gurobi_cl."
        ),
    )
    parser.add_argument(
        "--gurobi-cmd",
        default=None,
        help="Optional path to gurobi_cl when using --solver gurobi-cmd.",
    )
    parser.add_argument(
        "--time-limit",
        type=int,
        default=None,
        help="Optional solver time limit in seconds.",
    )
    parser.add_argument(
        "--max-machines",
        type=int,
        default=None,
        help="Number of candidate machines. Defaults to the number of jobs.",
    )
    parser.add_argument(
        "--no-symmetry-breaking",
        action="store_true",
        help="Disable simple machine-index symmetry-breaking constraints.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Hide solver log output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_file = resolve_input_csv_path(args.csv_file)
    output_prefix = args.output_prefix or f"{csv_file.stem}_ilp_busy_time"

    g, jobs = read_input_from_csv(csv_file)
    solver = make_solver(args)
    result = solve_busy_time_ilp(
        jobs=jobs,
        g=g,
        solver=solver,
        machine_count=args.max_machines,
        add_symmetry_breaking=not args.no_symmetry_breaking,
    )

    print_results(jobs, g, result)

    if result.status not in {"Optimal", "Feasible"}:
        raise SystemExit(f"No feasible schedule was produced. Solver status: {result.status}")

    output_files = save_all_results(
        jobs=jobs,
        g=g,
        result=result,
        solver_name=args.solver,
        output_prefix=output_prefix,
        output_base_dir=OUTPUTS_DIR,
    )

    print("\nResults saved to separate CSV files:")
    for label, filename in output_files.items():
        print(f"{label}: {filename}")


if __name__ == "__main__":
    main()
