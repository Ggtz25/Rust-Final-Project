use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
enum TaskType {
    Cpu,
    Io,
}

#[derive(Debug, Clone)]
struct Task {
    id: usize,
    task_type: TaskType,
    duration_ms: u64,
    cpu_usage: u32,
    created_at: Instant,
}

#[derive(Debug)]
struct Metrics {
    total_completed: usize,
    cpu_completed: usize,
    io_completed: usize,
    total_wait_ms: u128,
    total_turnaround_ms: u128,
    max_wait_ms: u128,
    max_wait_task_id: usize,
}

#[derive(Clone, Copy)]
enum Policy {
    Fifo,
    Optimized,
}

fn generate_task(id: usize) -> Task {
    // 70% IO, 30% CPU
    let is_io = id % 10 < 7;

    Task {
        id,
        task_type: if is_io { TaskType::Io } else { TaskType::Cpu },
        duration_ms: 200,
        cpu_usage: if is_io { 10 } else { 40 },
        created_at: Instant::now(),
    }
}

fn run_simulation(
    name: &str,
    policy: Policy,
    result_file_name: &str,
    monitor_file_name: &str,
) {
    let task_count = 1000;
    let worker_count = 8;

    println!("\n== {} ==", name);
    println!("1000 tasks, 70% IO / 30% CPU, 8 workers, cap 100%");

    let start_time = Instant::now();

    let queue = Arc::new(Mutex::new(VecDeque::<Task>::new()));
    let generation_finished = Arc::new(Mutex::new(false));

    let current_cpu = Arc::new(Mutex::new(0u32));
    let active_workers = Arc::new(Mutex::new(0usize));

    let metrics = Arc::new(Mutex::new(Metrics {
        total_completed: 0,
        cpu_completed: 0,
        io_completed: 0,
        total_wait_ms: 0,
        total_turnaround_ms: 0,
        max_wait_ms: 0,
        max_wait_task_id: 0,
    }));

    let monitor_done = Arc::new(Mutex::new(false));

    // Generator thread: creates 1000 tasks, one every 20ms
    {
        let queue_clone = Arc::clone(&queue);
        let finished_clone = Arc::clone(&generation_finished);

        thread::spawn(move || {
            for id in 1..=task_count {
                let task = generate_task(id);

                {
                    let mut q = queue_clone.lock().unwrap();
                    q.push_back(task);
                }

                thread::sleep(Duration::from_millis(20));
            }

            let mut finished = finished_clone.lock().unwrap();
            *finished = true;
        });
    }

    // Monitor thread: records CPU usage, queue length, and active workers every 10ms
    let monitor_handle = {
        let queue_clone = Arc::clone(&queue);
        let cpu_clone = Arc::clone(&current_cpu);
        let active_clone = Arc::clone(&active_workers);
        let done_clone = Arc::clone(&monitor_done);
        let monitor_file_name = monitor_file_name.to_string();

        thread::spawn(move || {
            let mut file = File::create(&monitor_file_name).unwrap();
            writeln!(file, "time_ms,cpu_usage,queue_length,active_workers").unwrap();

            let mut cpu_sum: u128 = 0;
            let mut active_sum: u128 = 0;
            let mut samples: u128 = 0;

            loop {
                let done = *done_clone.lock().unwrap();

                if done {
                    break;
                }

                let time_ms = start_time.elapsed().as_millis();

                let cpu = *cpu_clone.lock().unwrap();
                let queue_length = queue_clone.lock().unwrap().len();
                let active = *active_clone.lock().unwrap();

                writeln!(file, "{},{},{},{}", time_ms, cpu, queue_length, active).unwrap();

                cpu_sum += cpu as u128;
                active_sum += active as u128;
                samples += 1;

                thread::sleep(Duration::from_millis(10));
            }

            let avg_cpu = if samples > 0 {
                cpu_sum as f64 / samples as f64
            } else {
                0.0
            };

            let avg_active = if samples > 0 {
                active_sum as f64 / samples as f64
            } else {
                0.0
            };

            (avg_cpu, avg_active, samples)
        })
    };

    let mut handles = Vec::new();

    for _worker_id in 1..=worker_count {
        let queue_clone = Arc::clone(&queue);
        let finished_clone = Arc::clone(&generation_finished);
        let cpu_clone = Arc::clone(&current_cpu);
        let active_clone = Arc::clone(&active_workers);
        let metrics_clone = Arc::clone(&metrics);

        let handle = thread::spawn(move || loop {
            let task_option = {
                let mut q = queue_clone.lock().unwrap();
                let mut cpu = cpu_clone.lock().unwrap();

                match policy {
                    Policy::Fifo => {
                        if let Some(task) = q.pop_front() {
                            *cpu += task.cpu_usage;
                            Some(task)
                        } else {
                            None
                        }
                    }

                    Policy::Optimized => {
                        if let Some(task) = q.front() {
                            if *cpu + task.cpu_usage <= 100 {
                                let task = q.pop_front().unwrap();
                                *cpu += task.cpu_usage;
                                Some(task)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }
            };

            match task_option {
                Some(task) => {
                    {
                        let mut active = active_clone.lock().unwrap();
                        *active += 1;
                    }

                    let wait_ms = task.created_at.elapsed().as_millis();

                    thread::sleep(Duration::from_millis(task.duration_ms));

                    let turnaround_ms = task.created_at.elapsed().as_millis();

                    {
                        let mut cpu = cpu_clone.lock().unwrap();
                        *cpu -= task.cpu_usage;
                    }

                    {
                        let mut active = active_clone.lock().unwrap();
                        *active -= 1;
                    }

                    {
                        let mut m = metrics_clone.lock().unwrap();

                        m.total_completed += 1;
                        m.total_wait_ms += wait_ms;
                        m.total_turnaround_ms += turnaround_ms;

                        if wait_ms > m.max_wait_ms {
                            m.max_wait_ms = wait_ms;
                            m.max_wait_task_id = task.id;
                        }

                        match task.task_type {
                            TaskType::Cpu => m.cpu_completed += 1,
                            TaskType::Io => m.io_completed += 1,
                        }
                    }
                }

                None => {
                    let finished = *finished_clone.lock().unwrap();
                    let empty = queue_clone.lock().unwrap().is_empty();

                    if finished && empty {
                        break;
                    }

                    thread::sleep(Duration::from_millis(10));
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    {
        let mut done = monitor_done.lock().unwrap();
        *done = true;
    }

    let (avg_cpu_usage, avg_workers_active, monitor_samples) = monitor_handle.join().unwrap();

    let total_runtime_ms = start_time.elapsed().as_millis();
    let makespan_ms = total_runtime_ms;
    let final_metrics = metrics.lock().unwrap();

    let avg_wait = final_metrics.total_wait_ms as f64 / final_metrics.total_completed as f64;
    let avg_turnaround =
        final_metrics.total_turnaround_ms as f64 / final_metrics.total_completed as f64;

    let results = format!(
        "== {} ==\n\
         1000 tasks, 70% IO / 30% CPU, 8 workers, cap 100%\n\n\
         — results —\n\
         total runtime           : {} ms\n\
         makespan                : {} ms\n\
         tasks completed         : {} (IO={}, CPU={})\n\
         avg wait time           : {:.2} ms\n\
         avg turnaround time     : {:.2} ms\n\
         max wait time           : {} ms (task #{})\n\
         avg CPU usage           : {:.2} %\n\
         avg workers active      : {:.2} / {}\n\
         monitor samples         : {}\n\
         monitor csv             : {}\n",
        name,
        total_runtime_ms,
        makespan_ms,
        final_metrics.total_completed,
        final_metrics.io_completed,
        final_metrics.cpu_completed,
        avg_wait,
        avg_turnaround,
        final_metrics.max_wait_ms,
        final_metrics.max_wait_task_id,
        avg_cpu_usage,
        avg_workers_active,
        worker_count,
        monitor_samples,
        monitor_file_name
    );

    println!("{}", results);

    let mut result_file = File::create(result_file_name).unwrap();
    write!(result_file, "{}", results).unwrap();
}

fn main() {
    run_simulation(
        "FIFO simulation",
        Policy::Fifo,
        "fifo_results.txt",
        "fifo_monitor_log.csv",
    );

    run_simulation(
        "Optimized simulation",
        Policy::Optimized,
        "optimized_results.txt",
        "optimized_monitor_log.csv",
    );
}