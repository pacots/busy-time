import csv
from dataclasses import dataclass
from typing import List, Tuple, Dict
import random
import copy

@dataclass
class Job:
    job_id: str
    release: int
    deadline: int
    p: int  # processing time

@dataclass
class Interval:
    start: int
    end: int
    jobs: List[str]

    def length(self):
        return self.end - self.start


# -----------------------------
# Parsing
# -----------------------------
def read_instance(filename):
    jobs = []
    capacity = None

    with open(filename, 'r') as f:
        reader = csv.reader(f)
        lines = list(reader)

    capacity = int(lines[2][0])

    for row in lines[4:]:
        if not row:
            continue
        job_id, r, d, p = row
        jobs.append(Job(job_id.strip(), int(r), int(d), int(p)))

    return jobs, capacity


# -----------------------------
# Greedy Initial Solution
# -----------------------------
def greedy_schedule(jobs: List[Job], g: int) -> List[Interval]:
    jobs = sorted(jobs, key=lambda j: j.release)
    intervals = []

    for job in jobs:
        placed = False
        for interval in intervals:
            if interval.start >= job.release and interval.end <= job.deadline:
                if len(interval.jobs) < g:
                    interval.jobs.append(job.job_id)
                    placed = True
                    break

        if not placed:
            intervals.append(Interval(job.release, job.release + job.p, [job.job_id]))

    return intervals


# -----------------------------
# Cost = total busy time
# -----------------------------
def busy_time(intervals: List[Interval]) -> int:
    return sum(i.length() for i in intervals)


# -----------------------------
# Feasibility check (simple)
# -----------------------------
def can_pack(jobs: List[Job], start: int, end: int, g: int) -> bool:
    # crude check: total processing vs capacity*time
    total_p = sum(j.p for j in jobs)
    return total_p <= g * (end - start)


# -----------------------------
# Repack jobs into k intervals
# -----------------------------
def repack_jobs(jobs: List[Job], k: int, g: int) -> List[Interval]:
    jobs = sorted(jobs, key=lambda j: j.release)
    intervals = []

    for _ in range(k):
        intervals.append(Interval(0, 0, []))

    for job in jobs:
        for interval in intervals:
            if (interval.start == 0 and interval.end == 0):
                interval.start = job.release
                interval.end = job.release + job.p
                interval.jobs.append(job.job_id)
                break
            elif (job.release <= interval.start and job.deadline >= interval.end):
                if len(interval.jobs) < g:
                    interval.jobs.append(job.job_id)
                    break

    return intervals


# -----------------------------
# Local Search
# -----------------------------
def local_search(jobs: List[Job], g: int, b: int, max_iter=1000):
    current = greedy_schedule(jobs, g)

    for _ in range(max_iter):
        if len(current) <= b:
            break

        chosen = random.sample(current, b)

        # collect jobs
        job_ids = []
        for interval in chosen:
            job_ids.extend(interval.jobs)

        job_map = {j.job_id: j for j in jobs}
        selected_jobs = [job_map[jid] for jid in job_ids]

        # attempt repack into b-1 intervals
        new_intervals = repack_jobs(selected_jobs, b - 1, g)

        old_cost = sum(i.length() for i in chosen)
        new_cost = sum(i.length() for i in new_intervals)

        if new_cost < old_cost:
            # accept move
            current = [i for i in current if i not in chosen]
            current.extend(new_intervals)

    return current


# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    jobs, g = read_instance("inputs/input1.csv")

    solution = local_search(jobs, g, b=1)

    print("Final intervals:")
    for i in solution:
        print(i)

    print("Total busy time:", busy_time(solution))
