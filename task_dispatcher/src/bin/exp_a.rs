use rand::{SeedableRng, Rng};
use rand::rngs::StdRng;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskKind { Cpu, Io }

#[derive(Debug, Clone)]
pub struct Task {
    id: usize,
    kind: TaskKind,
    cpu_cost: f64,
    duration_ms: u64,
    arrival_time_ms: u64,
    start_time_ms: Option<u64>,
    finish_time_ms: Option<u64>,
}

pub struct MonitorSnapshot {
    cpu_consumption: f64,
    active_workers: usize,
}

fn generate_tasks() -> Vec<Task> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut tasks = Vec::new();
    for i in 0..1000 {
        let kind = if rng.gen_bool(0.7) { TaskKind::Io } else { TaskKind::Cpu };
        let cpu_cost = match kind {
            TaskKind::Cpu => 35.0,
            TaskKind::Io => 10.0,
        };
        tasks.push(Task {
            id: i,
            kind,
            cpu_cost,
            duration_ms: 200,
            arrival_time_ms: (i as u64) * 20,
            start_time_ms: None,
            finish_time_ms: None,
        });
    }
    tasks
}

fn print_results(completed: &[Task], snapshots: &[MonitorSnapshot], total_ms: u64) {
    let n = snapshots.len() as f64;
    let avg_cpu = snapshots.iter().map(|s| s.cpu_consumption).sum::<f64>() / n;
    let wait_times: Vec<u64> = completed.iter().map(|t| t.start_time_ms.unwrap() - t.arrival_time_ms).collect();
    let avg_wait = wait_times.iter().sum::<u64>() as f64 / completed.len() as f64;

    println!("=== ExpA: Strict FIFO Baseline ===");
    println!("Makespan (Total Runtime): {:>8} ms", total_ms);
    println!("Avg CPU Consumption:      {:>8.2}%", avg_cpu);
    println!("Avg Wait Time (All):      {:>8.2} ms", avg_wait);
    println!("Max Wait Time:            {:>8} ms", wait_times.iter().max().unwrap_or(&0));
}

fn main() {
    let start_instant = Instant::now();
    let queue = Arc::new(Mutex::new(VecDeque::<Option<Task>>::new()));
    let snapshots = Arc::new(Mutex::new(Vec::<MonitorSnapshot>::new()));
    let completed_tasks = Arc::new(Mutex::new(Vec::<Task>::new()));
    let active_workers = Arc::new(AtomicUsize::new(0));
    let cpu_load = Arc::new(Mutex::new(0.0f64));
    let done = Arc::new(Mutex::new(false));

    // --- Sender Thread ---
    let q_sender = Arc::clone(&queue);
    let d_sender = Arc::clone(&done);
    let sender_handle = thread::spawn(move || {
        let tasks = generate_tasks();
        for (i, task) in tasks.into_iter().enumerate() {
            let target_ms = (i as u64) * 20;
            let elapsed_ms = start_instant.elapsed().as_millis() as u64;
            if target_ms > elapsed_ms {
                thread::sleep(Duration::from_millis(target_ms - elapsed_ms));
            }
            q_sender.lock().unwrap().push_back(Some(task));
        }
        for _ in 0..8 { q_sender.lock().unwrap().push_back(None); }
        *d_sender.lock().unwrap() = true;
    });

    // --- Worker Threads (Strict FIFO) ---
    let mut worker_handles = Vec::new();
    for _ in 0..8 {
        let q_w = Arc::clone(&queue);
        let act_w = Arc::clone(&active_workers);
        let load_w = Arc::clone(&cpu_load);
        let comp_w = Arc::clone(&completed_tasks);

        worker_handles.push(thread::spawn(move || {
            loop {
                let mut q = q_w.lock().unwrap();
                if let Some(front_opt) = q.front() {
                    match front_opt {
                        Some(task) => {
                            let mut load = load_w.lock().unwrap();
                            if *load + task.cpu_cost <= 100.0 {
                                *load += task.cpu_cost;
                                let mut t = q.pop_front().unwrap().unwrap();
                                drop(load); drop(q); // Release locks to work

                                t.start_time_ms = Some(start_instant.elapsed().as_millis() as u64);
                                act_w.fetch_add(1, Ordering::SeqCst);
                                thread::sleep(Duration::from_millis(t.duration_ms));
                                act_w.fetch_sub(1, Ordering::SeqCst);
                                t.finish_time_ms = Some(start_instant.elapsed().as_millis() as u64);
                                
                                *load_w.lock().unwrap() -= t.cpu_cost;
                                comp_w.lock().unwrap().push(t);
                            } else {
                                // HoL Blocking: release locks and wait
                                drop(load); drop(q);
                                thread::sleep(Duration::from_millis(1));
                            }
                        }
                        None => { q.pop_front(); break; }
                    }
                } else {
                    drop(q);
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }));
    }

    // --- Monitor Thread ---
    let act_m = Arc::clone(&active_workers);
    let load_m = Arc::clone(&cpu_load);
    let d_m = Arc::clone(&done);
    let snap_m = Arc::clone(&snapshots);
    let monitor_handle = thread::spawn(move || {
        loop {
            if *d_m.lock().unwrap() && act_m.load(Ordering::SeqCst) == 0 { break; }
            {
                let mut s = snap_m.lock().unwrap();
                s.push(MonitorSnapshot {
                    cpu_consumption: *load_m.lock().unwrap(),
                    active_workers: act_m.load(Ordering::SeqCst),
                });
            }
            thread::sleep(Duration::from_millis(10));
        }
    });

    sender_handle.join().unwrap();
    for h in worker_handles { h.join().unwrap(); }
    monitor_handle.join().unwrap();
    print_results(&completed_tasks.lock().unwrap(), &snapshots.lock().unwrap(), start_instant.elapsed().as_millis() as u64);
}