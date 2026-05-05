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
            TaskKind::Cpu => 0.35,
            TaskKind::Io => 0.10,
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

fn main() {
    let start = std::time::Instant::now();
    let queue = Arc::new(Mutex::new(TaskQueue::new()));
    let done = Arc::new(Mutex::new(false));
    let monitor_data = Arc::new(Mutex::new(MonitorData { snapshots: Vec::new() }));
    let active_workers = Arc::new(AtomicUsize::new(0));
    let cpu_load = Arc::new(Mutex::new(0.0f64));

    // --- Sender Thread ---
    let queue_sender = Arc::clone(&queue);
    let done_sender = Arc::clone(&done);
    let sender_handle = thread::spawn(move || {
        let tasks = generate_tasks();
        for task in tasks {
            thread::sleep(Duration::from_millis(20));
            queue_sender.lock().unwrap().push(task);
        }
        *done_sender.lock().unwrap() = true;
    });
    
    // --- Worker Threads ---
    let mut worker_handles = Vec::new();
    for _ in 0..8 {
        let queue_worker = Arc::clone(&queue);
        let done_worker = Arc::clone(&done);
        let active_clone = Arc::clone(&active_workers);
        let cpu_clone = Arc::clone(&cpu_load);
        
        let handle = thread::spawn(move || {
            loop {
                let task = queue_worker.lock().unwrap().pop();
                
                match task {
                    Some(t) => {
                        // Track worker start
                        active_clone.fetch_add(1, Ordering::SeqCst);
                        *cpu_clone.lock().unwrap() += t.cpu_cost;
                        
                        thread::sleep(Duration::from_millis(t.duration_ms));
                        
                        // Track worker finish
                        active_clone.fetch_sub(1, Ordering::SeqCst);
                        *cpu_clone.lock().unwrap() -= t.cpu_cost;
                    }
                    None => {
                        if *done_worker.lock().unwrap() {
                            break;
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        });
        worker_handles.push(handle);
    }

    // --- Monitor Thread ---
    let active_workers_monitor = Arc::clone(&active_workers);
    let cpu_load_monitor = Arc::clone(&cpu_load);
    let queue_monitor = Arc::clone(&queue);
    let done_monitor = Arc::clone(&done);
    let monitor_data_monitor = Arc::clone(&monitor_data);
    
    let monitor_handle = thread::spawn(move || {
        loop {
            let is_done = *done_monitor.lock().unwrap();
            let is_empty = queue_monitor.lock().unwrap().len() == 0;
            
            if is_done && is_empty {
                break;
            }

            {
                let mut data = monitor_data_monitor.lock().unwrap();
                data.snapshots.push(MonitorSnapshot {
                    time_ms: start.elapsed().as_millis() as u64,
                    cpu_consumption: *cpu_load_monitor.lock().unwrap(),
                    active_workers: active_workers_monitor.load(Ordering::SeqCst),
                });
            }

            thread::sleep(Duration::from_millis(10));
        }
    });
    
    // --- Joins ---
    sender_handle.join().unwrap();
    for h in worker_handles { h.join().unwrap(); }
    monitor_handle.join().unwrap();

    println!("Simulation complete. Captured {} snapshots.", monitor_data.lock().unwrap().snapshots.len());
}