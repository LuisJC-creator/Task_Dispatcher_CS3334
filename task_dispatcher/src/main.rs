use rand::{SeedableRng, Rng};
use rand::rngs::StdRng;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy)]
pub enum TaskKind {
    Cpu,
    Io,
}

#[derive(Debug, Clone)]
pub struct Task {
    id: usize,
    kind: TaskKind,
    arrival_time_ms: u64,
    duration_ms: u64,
    cpu_cost: f64,
}

pub struct TaskQueue {
    queue: VecDeque<Task>,
}

impl TaskQueue {
    pub fn new() -> Self {
        TaskQueue { queue: VecDeque::new() }
    }
    pub fn push(&mut self, task: Task) {
        self.queue.push_back(task);
    }
    pub fn pop(&mut self) -> Option<Task> {
        self.queue.pop_front()
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

pub struct MonitorSnapshot {
    time_ms: u64,
    cpu_consumption: f64,
    active_workers: usize,
}

pub struct MonitorData {
    snapshots: Vec<MonitorSnapshot>,
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
            arrival_time_ms: (i as u64) * 20,
            duration_ms: 200,
        });
    }
    tasks
}

fn print_results(monitor_data: &MonitorData, total_time_ms: u64) {
    let snapshots = &monitor_data.snapshots;
    let n = snapshots.len() as f64;

    let avg_cpu = snapshots.iter().map(|s| s.cpu_consumption).sum::<f64>() / n;
    let avg_workers = snapshots.iter().map(|s| s.active_workers as f64).sum::<f64>() / n;
    let max_cpu = snapshots.iter().map(|s| s.cpu_consumption).fold(0.0f64, f64::max);

    println!("=== Simulation Results (Partitioned Pools) ===");
    println!("Total runtime:        {} ms", total_time_ms);
    println!("Avg CPU consumption:  {:.3}", avg_cpu);
    println!("Max CPU consumption:  {:.3}", max_cpu);
    println!("Avg active workers:   {:.2}", avg_workers);
}

fn main() {
    let start = std::time::Instant::now();
    let cpu_queue = Arc::new(Mutex::new(TaskQueue::new()));
    let io_queue = Arc::new(Mutex::new(TaskQueue::new()));
    let done = Arc::new(Mutex::new(false));
    let monitor_data = Arc::new(Mutex::new(MonitorData { snapshots: Vec::new() }));
    let active_workers = Arc::new(AtomicUsize::new(0));
    let cpu_load = Arc::new(Mutex::new(0.0f64));

    // --- Sender Thread ---
    let cpu_q_sender = Arc::clone(&cpu_queue);
    let io_q_sender = Arc::clone(&io_queue);
    let done_sender = Arc::clone(&done);
    let sender_handle = thread::spawn(move || {
        let tasks = generate_tasks();
        for task in tasks {
            thread::sleep(Duration::from_millis(20));
            match task.kind {
                TaskKind::Cpu => cpu_q_sender.lock().unwrap().push(task),
                TaskKind::Io => io_q_sender.lock().unwrap().push(task),
            }
        }
        *done_sender.lock().unwrap() = true;
    });
    
    let mut worker_handles = Vec::new();

    // --- 2 Dedicated CPU Workers ---
    for _ in 0..2 {
        let q = Arc::clone(&cpu_queue);
        let d = Arc::clone(&done);
        let active = Arc::clone(&active_workers);
        let load_mtx = Arc::clone(&cpu_load);
        
        worker_handles.push(thread::spawn(move || {
            loop {
                let task = {
                    let mut load = load_mtx.lock().unwrap();
                    if *load + 35.0 <= 100.0 {
                        let mut q_lock = q.lock().unwrap();
                        if let Some(t) = q_lock.pop() {
                            *load += 35.0;
                            Some(t)
                        } else { None }
                    } else { None }
                };

                match task {
                    Some(t) => {
                        active.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(t.duration_ms));
                        active.fetch_sub(1, Ordering::SeqCst);
                        *load_mtx.lock().unwrap() -= 35.0;
                    }
                    None => {
                        if *d.lock().unwrap() && q.lock().unwrap().len() == 0 { break; }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }));
    }

    // --- 6 Dedicated IO Workers ---
    for _ in 0..6 {
        let q = Arc::clone(&io_queue);
        let d = Arc::clone(&done);
        let active = Arc::clone(&active_workers);
        let load_mtx = Arc::clone(&cpu_load);
        
        worker_handles.push(thread::spawn(move || {
            loop {
                let task = {
                    let mut load = load_mtx.lock().unwrap();
                    if *load + 10.0 <= 100.0 {
                        let mut q_lock = q.lock().unwrap();
                        if let Some(t) = q_lock.pop() {
                            *load += 10.0;
                            Some(t)
                        } else { None }
                    } else { None }
                };

                match task {
                    Some(t) => {
                        active.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(t.duration_ms));
                        active.fetch_sub(1, Ordering::SeqCst);
                        *load_mtx.lock().unwrap() -= 10.0;
                    }
                    None => {
                        if *d.lock().unwrap() && q.lock().unwrap().len() == 0 { break; }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }));
    }

    // --- Monitor Thread ---
    let active_monitor = Arc::clone(&active_workers);
    let load_monitor = Arc::clone(&cpu_load);
    let cq_monitor = Arc::clone(&cpu_queue);
    let iq_monitor = Arc::clone(&io_queue);
    let d_monitor = Arc::clone(&done);
    let data_monitor = Arc::clone(&monitor_data);
    
    let monitor_handle = thread::spawn(move || {
        loop {
            let is_done = *d_monitor.lock().unwrap();
            let is_empty = cq_monitor.lock().unwrap().len() == 0 && iq_monitor.lock().unwrap().len() == 0;
            if is_done && is_empty { break; }

            {
                let mut data = data_monitor.lock().unwrap();
                data.snapshots.push(MonitorSnapshot {
                    time_ms: start.elapsed().as_millis() as u64,
                    cpu_consumption: *load_monitor.lock().unwrap(),
                    active_workers: active_monitor.load(Ordering::SeqCst),
                });
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    
    sender_handle.join().unwrap();
    for h in worker_handles { h.join().unwrap(); }
    monitor_handle.join().unwrap();

    let total_time_ms = start.elapsed().as_millis() as u64;
    let data = monitor_data.lock().unwrap();
    print_results(&data, total_time_ms);
}