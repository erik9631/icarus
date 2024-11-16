use crossbeam::queue::ArrayQueue;
use std::cell::Cell;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;

thread_local! {
   static THREAD_LOCAL_ID: Cell<usize> = Cell::new(0);
}

struct Task {
    pub id: usize,
    pub func: Box<dyn FnOnce(&Executor) + Send>,
}

impl Task {
    pub fn new(func: Box<dyn FnOnce(&Executor) + Send>, id: usize) -> Self {
        Self { func, id }
    }
}

pub struct Executor {
    thread_queues: Vec<ArrayQueue<Task>>,
    id_gen: AtomicUsize,
    thread_waker: Condvar,
    waker_mutex: Mutex<bool>,
    join_handles: Vec<JoinHandle<()>>,
}
impl Executor {
    fn thread_loop(&self, thread_queues: Vec<ArrayQueue<Task>>, thread_id: usize, thread_waker: &Condvar, waker_mutex: &Mutex<bool>) {
        THREAD_LOCAL_ID.set(thread_id);
        let local_id = THREAD_LOCAL_ID.get();

        loop {
            // First take tasks from its own queue
            while let Some(task) = thread_queues[local_id].pop() {
                (task.func)(&self);
            }

            // Then take tasks from other queues
            for queue in thread_queues.iter() {
                if let Some(task) = queue.pop() {
                    if let Err(_) = thread_queues[local_id].push(task) {
                        panic!("Failed to push to local queue");
                    }
                }
            }

            // Park the thread until woken up by a new task
            let waker_guard = waker_mutex.lock().unwrap();
            let state = *waker_guard;
            let _result = thread_waker
                .wait_while(waker_guard, |guard| *guard)
                .expect("Thread waker failed to park");
            // Drain the queue
            if !state {
                while let Some(task) = thread_queues[local_id].pop() {
                    (task.func)(&self);
                }
                break;
            }
        }
    }
    pub fn new(thread_count: usize) -> Self {
        let mut executor = Self {
            thread_queues: Vec::with_capacity(thread_count),
            id_gen: AtomicUsize::new(0),
            thread_waker: Condvar::new(),
            waker_mutex: Mutex::new(true),
            join_handles: Vec::with_capacity(thread_count),
        };
        for _ in 0..thread_count {
            executor.thread_queues.push(ArrayQueue::new(1024));
        }
        executor
    }
    pub fn submit(&self, func: Box<dyn FnOnce(&Executor) + Send>) -> usize {
        let result = self.id_gen.load(std::sync::atomic::Ordering::Acquire);
        self.thread_queues[THREAD_LOCAL_ID.get()].push(Task::new(func, result)).ok();
        self.thread_waker.notify_all();
        self.id_gen.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        result
    }

    pub fn close(&mut self) {
        let mut guard = self.waker_mutex.lock().unwrap();
        *guard = false;
        self.thread_waker.notify_all();

        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
