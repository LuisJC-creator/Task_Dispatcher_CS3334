# Task_Dispatcher_CS3334
Repo for the Task_Dispatcher final project for UTRGV CS3334 Systems Programming Course

# Concurrent Task Dispatcher

A multithreaded task dispatcher simulation built in Rust for CS3334 Systems Programming.

## Build and Run
// FROM: task_dispatcher folder
```bash
cargo build
cargo run
```

## Design Summary

The system simulates 1000 tasks arriving every 20ms, dispatched to a pool of 8 worker 
threads through a shared FIFO queue. Tasks are either CPU-bound (35% CPU cost) or IO-bound 
(10% CPU cost), generated at a 70/30 ratio. A monitor thread samples system state every 
10ms. Experiment B is the optimized version — IMPLEMENTATION TBD

## Experiments

- **Experiment A (FIFO queue (even though it's using rust's std deque, it's used like a regular queue))** Total runtime: 39087 ms
- **Experiment B (Optimization)**: TBD

Full outputs for instances of exp. A and B in `root/task_dispatcher/results/`

## Tool Use Disclosure

- **Tools used**: Claude (Anthropic AI assistant), Gemini, [RustDocs](https://doc.rust-lang.org/std/index.html).
- **Help provided**: Guided debugging of Rust syntax after implementing from doc examples, threading architecture design, 
  and Arc/Mutex usage patterns.
- **Advice accepted**: Lock ordering strategy to avoid deadlocks when acquiring 
  cpu_load and queue mutexes simultaneously
- **Advice rejected**: Suggested a manual circular array deque implementation — 
  chose std::collections::VecDeque instead to keep focus on the scheduling logic