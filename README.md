# Concurrent Task Dispatcher Simulator

## Project Overview

This project is a concurrent task scheduling simulator written in Rust for a Systems Programming course.

The system simulates CPU-bound and IO-bound tasks using a queue-based architecture with a bounded worker pool. Tasks are generated over time and dispatched to worker threads using different scheduling strategies.

The project compares:
1. FIFO scheduling
2. CPU-aware optimized scheduling

The goal is to evaluate runtime behavior, CPU utilization, wait times, and overall scheduling efficiency.

---

## Features

- Concurrent worker pool
- Queue-based task scheduling
- CPU-bound and IO-bound task simulation
- FIFO scheduling policy
- CPU-aware optimized scheduling policy
- 1000-task simulations
- 20ms task generation intervals
- Metrics collection
- Monitor thread with CSV logging
- Automatic experiment output files
- Clean thread shutdown

---

## Architecture Summary

The system contains several concurrent components:

### Task Generator
Creates tasks over time at 20ms intervals and inserts them into the shared queue.

### Shared Queue
Stores waiting tasks before dispatching them to workers.

### Worker Pool
A bounded set of worker threads executes tasks concurrently.

### Scheduler
Controls task execution order:
- FIFO mode executes tasks immediately in queue order
- Optimized mode prevents CPU usage from exceeding 100%

### Monitor Thread
Samples:
- CPU usage
- queue length
- active workers

The monitor writes data into CSV files for analysis.

---

## Task Types

### IO Tasks
- Simulated with sleep calls
- Use approximately 10% CPU

### CPU Tasks
- Simulated with sleep calls
- Use approximately 40% CPU

Workload distribution:
- 70% IO tasks
- 30% CPU tasks

---

## Experiments

### Experiment 1 — FIFO Scheduling
Tasks are executed strictly in queue order without CPU-aware optimization.

### Experiment 2 — Optimized Scheduling
Tasks are only dispatched if total CPU usage remains below 100%.

The optimized scheduler reduced total runtime and improved CPU utilization.

---

## Metrics Collected

- Total runtime
- Makespan
- Tasks completed
- Average wait time
- Average turnaround time
- Maximum wait time
- Average CPU usage
- Average workers active

---

## Files Generated

Running the program creates:

- `fifo_results.txt`
- `optimized_results.txt`
- `fifo_monitor_log.csv`
- `optimized_monitor_log.csv`

---

## Build and Run

### Build

```bash
cargo build