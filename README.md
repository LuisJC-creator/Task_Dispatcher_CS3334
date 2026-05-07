# Task_Dispatcher_CS3334

Final project for UTRGV CS3334 Systems Programming. A multithreaded task dispatcher simulation built in Rust, comparing two scheduling policies under a CPU (simulated) utilization cap.

## Build and Run

From the `task_dispatcher/` directory:

```bash
cargo build --release
cargo run --release --bin exp_a    # baseline: head-of-queue admission
cargo run --release --bin exp_b    # optimized: CPU-preferring dispatch
```

To save full output to the results files:

```bash
cargo run --release --bin exp_a > results/ExperimentA.txt
cargo run --release --bin exp_b > results/ExperimentB.txt
```

## Project Structure

```
task_dispatcher/
├── Cargo.toml
├── src/
│   └── bin/
│       ├── exp_a.rs        # baseline FIFO scheduler
│       └── exp_b.rs        # CPU-preferring scheduler
└── results/
    ├── ExperimentA.txt
    └── ExperimentB.txt
```

Both binaries share the same task generation, sender, monitor, and worker structure. The only difference between them is the dispatch decision the worker makes when picking the next task, which isolates the scheduling policy as the single independent variable in the comparison.

## Design Summary

The system simulates 1000 tasks arriving every 20ms over a 20-second window. Tasks are generated with a fixed seed and split 70/30 IO/CPU, where IO tasks consume 10% CPU and CPU tasks consume 35%. Each task simulates 200ms of work via `thread::sleep`. A hard cap of 100% CPU utilization is enforced at the dispatch decision: a worker may only admit a task if `current_load + task.cpu_cost <= 100`.

Three thread roles cooperate through shared state:

- **Sender thread** generates tasks and pushes them into the shared queue at fixed 20ms wall-clock intervals using absolute timing (`target = start + i * 20ms`) so that lock contention does not cause cumulative drift in the arrival schedule.
- **Worker pool** of 8 threads. Each worker repeatedly acquires the load and queue locks, asks the queue for an admissible task, runs it for 200ms, then releases the load it consumed.
- **Monitor thread** samples `cpu_consumption` and `active_workers` every 10ms into a shared vector. End-of-run aggregation produces the metrics reported below.

Shared state is protected by three primitives: `Arc<Mutex<VecDeque<Task>>>` around the queue, `Arc<Mutex<f64>>` around the CPU load counter, and an `Arc<AtomicUsize>` for active worker count (atomic because workers update it on the hot path and the monitor reads it without contention). Workers acquire the queue lock and `cpu_load` lock together briefly during the dispatch decision in a fixed order to prevent deadlock, then release both before the 200ms simulated work to maximize parallelism.

Shutdown uses a `done` boolean set by the sender once the last task is pushed, plus `None` sentinel values pushed onto the queue (one per worker). Workers exit on receiving `None`. The monitor exits when `done` is set and all workers are idle.

## Experiments

Both experiments run the same workload — the comparison isolates the effect of the scheduling policy on a 70/30 IO/CPU mix under a 100% CPU cap.

### Experiment A — Basic FIFO with head admission

The dispatcher checks only the front of the queue. If the front task fits the current load, it is dispatched. Otherwise the worker idles 1ms and retries. A heavy CPU task at the front blocks all 8 workers until enough load drains for it to fit, even when fittable tasks exist further back in the queue.

```
Makespan:               39079 ms
Avg CPU Consumption:    89.48%
Avg Active Workers:     5.12
Avg Wait Time (All):    9205.98 ms
Avg Wait Time (CPU):    9528.33 ms
Avg Wait Time (IO):     9068.49 ms
Max Wait Time:          18915 ms
```

**Interpretation:** A simple FIFO queue that takes tasks in arrival order. Workers pick up the first task in the queue if it does not exceed the CPU usage cap. Wait times are nearly equal across task types because no policy distinguishes them — both kinds wait the same amount in arrival order.

### Experiment B — CPU-preferring dispatch

The dispatcher first scans the queue for a CPU task that fits the current load. If one is found, it is dispatched ahead of any IO tasks at the head. If no CPU task fits (either because none exist or the load is too high to admit one), the dispatcher falls back to the head of the queue and dispatches the IO task there if it fits. The hypothesis is that CPU tasks at 35% load each represent more "work units per utilization slot" than IO tasks at 10%, so prioritizing them packs the 100% cap more efficiently and drains the heavier work earlier.

```
Makespan:               36343 ms
Avg CPU Consumption:    96.23%
Avg Active Workers:     5.51
Avg Wait Time (All):    9461.40 ms
Avg Wait Time (CPU):    4725.83 ms
Avg Wait Time (IO):    11481.28 ms
Max Wait Time:          17352 ms
```

**Interpretation:** ExpB beats ExpA on the optimization target by 2.7 seconds (7% reduction in makespan). Average CPU utilization rises from 89.48% to 96.23% and average active workers from 5.12 to 5.51, indicating that the CPU-preference policy keeps the system closer to the steady-state ceiling defined by the cap. The wait-time data confirms the mechanism: CPU tasks dispatch about 2x faster (9528ms → 4725ms) because workers actively pull them out of the queue, while IO tasks wait longer (9068ms → 11481ms) because they are deferred whenever a fittable CPU task exists. The maximum wait time also drops from 18915ms to 17352ms, indicating the policy does not produce a long tail. The trade-off is acceptable for this workload because the 30% CPU tasks represent the majority of total cap-load (300 × 35 = 10500 load-units vs 700 × 10 = 7000 load-units), so accelerating their drain shortens the post-arrival cleanup phase that dominates ExpA's runtime.

## Tool Use Disclosure

Per the project's outside-help policy, this section discloses AI assistant usage during development.

**Tools used:** Claude (Anthropic), Gemini (Google), Grok (xAI), ChatGPT(OpenAI) and the [Rust standard library documentation](https://doc.rust-lang.org/std/index.html).

**Help provided:** Threading architecture design (worker pool composition, lock ordering), Rust syntax debugging for `Arc<Mutex<T>>` and `AtomicUsize` patterns, scheduling policy iteration for ExpB, and analytical framing for the experiment comparison. Substantial back-and-forth went into testing scheduling alternatives (two-queue partitioned pools, hybrid floater workers, capacity-reservation policies, mpsc channel dispatch, single-mutex scan-and-fit) before arriving at the CPU-preferring policy that beat the FIFO baseline.

**Advice accepted:** The lock-ordering strategy of acquiring locks in a fixed order to prevent deadlocks between worker dispatch and load-decrement paths. The `Arc<Mutex<T>>` shared-state pattern for the queue and load counter. The use of an `AtomicUsize` for the active worker counter to avoid mutex contention on a value updated on the hot path.

**Advice rejected / had to fix:** Multiple AI suggestions were tested and discarded. Gemini proposed that cumulative thread drift in the sender (relative `thread::sleep(20ms)` accumulating contention delays) was the bottleneck; the absolute-timing change was implemented and verified, but runtime did not improve under the wrong scheduling policy, ruling out drift as the primary cause. Grok proposed a Condvar-based producer-consumer to replace the polling loop, but inspection of the proposed code revealed a state-mutation bug (the predicate function was called twice in the same iteration, which would either fail or pop a different task on the second call); the polling-based design was retained. Multiple over-engineered designs (partitioned worker pools, hybrid pools with a "floater" worker, scan-and-fit with first-fits-anywhere) were proposed by Claude along with other llms and tested; none beat the FIFO baseline. A simple implementation "prefer CPU, then fill with I/O" was the correct call and every single LLM favored making the architecture increasingly complex. It's an interesting look into LLMs and how they operate, could suggest something about their training data relating to these types of operations. Likely the simulation constraints confused the models, but I'm just speculating.