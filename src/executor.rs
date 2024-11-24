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
    // Refactor make your own interior mutability wrapper so threads can share this without unsafe and with move semantics
    thread_queues: Vec<ArrayQueue<Task>>,
    id_gen: AtomicUsize,
    thread_waker: Condvar,
    waker_mutex: Mutex<bool>,
    join_handles: Vec<JoinHandle<()>>,
}

struct ExecutorPtr(*const Executor);
unsafe impl Send for ExecutorPtr {}
impl Executor {
    fn thread_loop(executor: ExecutorPtr, thread_id: usize) {
        THREAD_LOCAL_ID.set(thread_id);
        let local_id = THREAD_LOCAL_ID.get();
        println!("Thread loop {} started", thread_id);
        let executor = unsafe { &*executor.0 };
        let thread_queues = &executor.thread_queues;
        let thread_waker = &executor.thread_waker;
        let waker_mutex = &executor.waker_mutex;

        loop {
            // First take tasks from its own queue
            while let Some(task) = thread_queues[local_id].pop() {
                (task.func)(&executor);
            }

            // Then take tasks from other queues
            for queue in thread_queues.iter() {
                if let Some(task) = queue.pop() {
                    println!("Thread {} stealing task {} to queue", thread_id, task.id);
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
                println!("Thread loop {} draining", thread_id);
                while let Some(task) = thread_queues[local_id].pop() {
                    (task.func)(&executor);
                }
                println!("Thread loop {} finished", thread_id);
                break;
            }
        }
    }
    pub fn new(thread_count: usize) -> Box<Self> {
        let thread_loops = thread_count - 1;
        let mut executor = Box::new(Self {
            thread_queues: Vec::with_capacity(thread_count),
            id_gen: AtomicUsize::new(0),
            thread_waker: Condvar::new(),
            waker_mutex: Mutex::new(true),
            join_handles: Vec::with_capacity(thread_count),
        });
        for _ in 0..thread_count {
            executor.thread_queues.push(ArrayQueue::new(1024));
        }

        unsafe {
            for i in 0..thread_loops {
                let executor_ptr = ExecutorPtr(&*executor as *const Executor);
                let id = i + 1;
                executor
                    .join_handles
                    .push(std::thread::spawn(move || Self::thread_loop(executor_ptr, id)));
            }
        }
        executor
    }
    pub fn submit(&self, func: Box<dyn FnOnce(&Executor) + Send>) -> usize {
        let result = self.id_gen.load(std::sync::atomic::Ordering::Acquire);
        println!("Submitting task {} to thread {}", result, THREAD_LOCAL_ID.get());
        self.thread_queues[THREAD_LOCAL_ID.get()].push(Task::new(func, result)).ok();
        self.thread_waker.notify_all();
        self.id_gen.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        result
    }

    pub fn close(&mut self) {
        let mut guard = self.waker_mutex.lock().unwrap();
        *guard = false;
        self.thread_waker.notify_all();
        drop(guard);
        //Close yields till empty
        let executor_ptr = ExecutorPtr(&*self as *const Executor);
        Self::thread_loop(executor_ptr, 0);

        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
