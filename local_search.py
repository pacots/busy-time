from dataclasses import dataclass
from itertools import combinations
from typing import List, Tuple, Dict, Optional
import copy


@dataclass(frozen=True)
class Job:
    """
    Interval job for busy time scheduling.

    Each job has a fixed interval [release, deadline).
    """
    job_id: str
    release: int
    deadline: int

    @property
    def length(self) -> int:
        return self.deadline - self.release


@dataclass
class Machine:
    """
    A machine stores a set of interval jobs.
    """
    jobs: List[Job]

    def busy_time(self) -> int:
        """
        Returns the total length of the union of job intervals on this machine.
        """
        if not self.jobs:
            return 0

        intervals = sorted((job.release, job.deadline) for job in self.jobs)

        total = 0
        current_start, current_end = intervals[0]

        for start, end in intervals[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                total += current_end - current_start
                current_start, current_end = start, end

        total += current_end - current_start
        return total

    def is_feasible_with(self, job: Job, capacity: int) -> bool:
        """
        Checks whether adding a job to this machine violates capacity g.

        Capacity condition:
        At every time t, at most g jobs overlap on the machine.
        """
        candidate_jobs = self.jobs + [job]
        return is_feasible_machine(candidate_jobs, capacity)

    def add_job(self, job: Job) -> None:
        self.jobs.append(job)


def is_feasible_machine(jobs: List[Job], capacity: int) -> bool:
    """
    Checks whether a set of interval jobs can be assigned to one machine
    without exceeding capacity g at any time.
    """
    events = []

    for job in jobs:
        events.append((job.release, 1))
        events.append((job.deadline, -1))

    # Important:
    # End events should be processed before start events at the same time,
    # because intervals are [release, deadline).
    events.sort(key=lambda x: (x[0], x[1]))

    active = 0

    for _, change in events:
        active += change
        if active > capacity:
            return False

    return True


def total_busy_time(schedule: List[Machine]) -> int:
    """
    Total busy time across all machines.
    """
    return sum(machine.busy_time() for machine in schedule)


def first_fit_initial_schedule(jobs: List[Job], capacity: int) -> List[Machine]:
    """
    Builds an initial feasible schedule using First Fit.

    Jobs are sorted by release time, then deadline.
    Each job is placed into the first feasible machine.
    If no feasible machine exists, a new machine is opened.
    """
    machines: List[Machine] = []

    sorted_jobs = sorted(jobs, key=lambda j: (j.release, j.deadline, j.job_id))

    for job in sorted_jobs:
        placed = False

        for machine in machines:
            if machine.is_feasible_with(job, capacity):
                machine.add_job(job)
                placed = True
                break

        if not placed:
            machines.append(Machine(jobs=[job]))

    return machines


def greedy_repack(jobs: List[Job], capacity: int) -> List[Machine]:
    """
    Repack a group of jobs using a greedy minimum-increase rule.

    For each job, place it into the feasible machine that causes the smallest
    increase in busy time. If no machine can take it, open a new machine.
    """
    machines: List[Machine] = []

    # Sorting longer jobs first often gives better packings.
    sorted_jobs = sorted(
        jobs,
        key=lambda j: (-j.length, j.release, j.deadline, j.job_id)
    )

    for job in sorted_jobs:
        best_machine = None
        best_increase = None

        for machine in machines:
            if machine.is_feasible_with(job, capacity):
                old_cost = machine.busy_time()
                temp_jobs = machine.jobs + [job]
                new_cost = Machine(temp_jobs).busy_time()
                increase = new_cost - old_cost

                if best_increase is None or increase < best_increase:
                    best_increase = increase
                    best_machine = machine

        if best_machine is not None:
            best_machine.add_job(job)
        else:
            machines.append(Machine(jobs=[job]))

    return machines


def local_search_busy_time(
    jobs: List[Job],
    capacity: int,
    b: int = 2,
    max_iterations: int = 100
) -> List[Machine]:
    """
    Busy-time b-local search.

    Local move:
        Select up to b machines.
        Remove all jobs from those machines.
        Repack those jobs into a new set of machines.
        Accept the move if total busy time decreases.

    This is a practical local search implementation using greedy repacking.
    """
    schedule = first_fit_initial_schedule(jobs, capacity)

    print("Initial schedule cost:", total_busy_time(schedule))

    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        improvement_found = False

        current_cost = total_busy_time(schedule)
        machine_indices = range(len(schedule))

        # Try subsets of machines of size 1 through b.
        for subset_size in range(1, min(b, len(schedule)) + 1):
            for selected_indices in combinations(machine_indices, subset_size):
                selected_indices_set = set(selected_indices)

                old_machines = [schedule[i] for i in selected_indices]
                remaining_machines = [
                    copy.deepcopy(schedule[i])
                    for i in range(len(schedule))
                    if i not in selected_indices_set
                ]

                jobs_to_repack = []
                for machine in old_machines:
                    jobs_to_repack.extend(machine.jobs)

                old_local_cost = sum(machine.busy_time() for machine in old_machines)

                new_machines = greedy_repack(jobs_to_repack, capacity)
                new_local_cost = sum(machine.busy_time() for machine in new_machines)

                if new_local_cost < old_local_cost:
                    new_schedule = remaining_machines + new_machines
                    new_cost = total_busy_time(new_schedule)

                    if new_cost < current_cost:
                        schedule = new_schedule
                        improvement_found = True

                        print(
                            f"Iteration {iteration}: improved "
                            f"{current_cost} -> {new_cost}"
                        )

                        break

            if improvement_found:
                break

        if not improvement_found:
            print("No further local improvement found.")
            break

    return schedule


def print_schedule(schedule: List[Machine]) -> None:
    """
    Pretty-print the schedule.
    """
    print("\nFinal Schedule")
    print("==============")
    print("Total busy time:", total_busy_time(schedule))
    print("Number of machines:", len(schedule))

    for i, machine in enumerate(schedule, start=1):
        print(f"\nMachine {i}")
        print("Busy time:", machine.busy_time())

        for job in sorted(machine.jobs, key=lambda j: (j.release, j.deadline, j.job_id)):
            print(f"  Job {job.job_id}: [{job.release}, {job.deadline})")


if __name__ == "__main__":
    # Example interval jobs.
    # Each job is fixed in time: [release, deadline).
    jobs = [
        Job("J1", 0, 4),
        Job("J2", 1, 5),
        Job("J3", 2, 6),
        Job("J4", 5, 8),
        Job("J5", 6, 9),
        Job("J6", 0, 3),
        Job("J7", 3, 7),
        Job("J8", 7, 10),
    ]

    # Maximum number of overlapping jobs allowed on one machine.
    g = 2

    # Local search parameter.
    # Larger b gives a stronger search but takes longer.
    b = 2

    final_schedule = local_search_busy_time(
        jobs=jobs,
        capacity=g,
        b=b,
        max_iterations=100
    )

    print_schedule(final_schedule)