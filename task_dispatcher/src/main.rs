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
    // Added a front method to allow workers to peek at the task cost
    pub fn front(&self) -> Option<&Task> {
        self.queue.front()
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

    println!("=== Simulation Results ===");
    println!("Total runtime:        {} ms", total_time_ms);
    println!("Snapshots captured:   {}", snapshots.len());
    println!("Avg CPU consumption:  {:.3}", avg_cpu);
    println!("Max CPU consumption:  {:.3}", max_cpu);
    println!("Avg active workers:   {:.2}", avg_workers);
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
                let task = {
                    let mut current_load = cpu_clone.lock().unwrap(); // getting warned these two lines cause deadlock but not sure why
                    let mut q = queue_worker.lock().unwrap();
                    
                    match q.front() {
                        Some(t) if *current_load + t.cpu_cost <= 100.0 => { // CHANGES HERE:
                            // take task if room
                            *current_load += t.cpu_cost;
                            q.pop()
                        }
                        _ => None, // no cpu or queue empty
                    }
                }; 
                
                match task {
                    Some(t) => {
                        active_clone.fetch_add(1, Ordering::SeqCst);
                        
                        thread::sleep(Duration::from_millis(t.duration_ms));
                        
                        active_clone.fetch_sub(1, Ordering::SeqCst);
                        *cpu_clone.lock().unwrap() -= t.cpu_cost;
                    }
                    None => {
                        // checking if done
                        if *done_worker.lock().unwrap() && queue_worker.lock().unwrap().len() == 0 {
                            break;
                        }
                        // waiting for tasks here
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

    let total_time_ms = start.elapsed().as_millis() as u64;
    let data = monitor_data.lock().unwrap();
    print_results(&data, total_time_ms);
}