use crate::messages::Message;
use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam::queue::ArrayQueue;
use eta_algorithms::data_structs::bitmap::handle::Handle;
use eta_algorithms::data_structs::bitmap::Bitmap;
use std::cell::Cell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;

thread_local! {
   static THREAD_LOCAL_ID: Cell<usize> = Cell::new(0);
}

pub struct WakeData {
    handle: Vec<Handle>,
    thread_id: usize,
}

struct Waker {
    signal: Condvar,
    wake_src: AtomicI32,
    signal_mutex: Mutex<bool>,
}

impl Waker {
    pub fn new() -> Self {
        Self {
            signal: Condvar::new(),
            signal_mutex: Mutex::new(true),
            wake_src: AtomicI32::new(-1),
        }
    }

    pub fn wake_src(&self) -> i32 {
        self.wake_src.load(Relaxed)
    }
    pub fn wake(&self, wake_src: i32) {
        let mut signal_mutex = self.signal_mutex.lock().unwrap();
        *signal_mutex = false;
        self.wake_src.store(wake_src, Relaxed);
        self.signal.notify_all();
    }

    pub fn wait(&self) -> i32 {
        let signal_mutex = self.signal_mutex.lock().unwrap();
        let mut guard = self.signal.wait_while(signal_mutex, |signal_mutex| *signal_mutex).unwrap();
        *guard = true;
        let wake_src = self.wake_src.load(Relaxed);
        self.wake_src.store(-1, Relaxed);
        wake_src
    }
}

struct Task {
    pub id: usize,
    pub func: Box<dyn FnOnce(&Executor) + Send>,
    stolen: bool,
}

impl Task {
    pub fn steal(mut self) -> Self {
        self.stolen = true;
        self
    }
    pub fn new(func: Box<dyn FnOnce(&Executor) + Send>, id: usize) -> Self {
        Self { func, id, stolen: false }
    }
}

pub struct Executor {
    // Refactor make your own interior mutability wrapper so threads can share this without unsafe and with move semantics
    thread_queues: Vec<ArrayQueue<Task>>,
    id_gen: AtomicUsize,
    thread_wakers: Vec<Waker>,
    join_handles: Vec<JoinHandle<()>>,
    reactive_thread_handle: Option<JoinHandle<()>>,
    quit: AtomicBool,
    reactive_sender: Sender<Message>,
}

// TODO: Make a reactive thread
// We need to make a reactive thread to which you can register a waker.
// The waker will be nothing but a barrier (So a condvar)
// The execution threads will communicate with the main thread and they will push messages (Not tasks) onto the reactive thread.
// The reactive thread will wake the waker if it receives the correct message with the correct value.
// Generally the reactive thread could be responsible for the futures so maybe we don't even need an atomic bitmap.
// consider introducing a flag for if the task was stolen or not. Only stolen tasks need to go through the reactive thread.
struct ExecutorPtr(*const Executor);
unsafe impl Send for ExecutorPtr {}
impl Executor {
    const TASK_COUNT: usize = 1024;

    fn reactive_thread_loop(executor_ptr: ExecutorPtr, r: Receiver<Message>) {
        // Optimize think about how to make the task finished more efficient so we don't have to iterate over the whole vec
        let mut task_states = Bitmap::new(Self::TASK_COUNT * 2);
        let executor = unsafe { &*executor_ptr.0 };
        let mut wait_data_vec = Vec::new();
        let thread_wakers = executor.thread_wakers.as_slice();

        loop {
            let message = r.recv().expect("Failed to receive message");
            match message {
                Message::WakeOnHandleComplete(wake_data) => {
                    println!("Reactive thread received wake on handle complete");
                    wait_data_vec.push(wake_data);
                }
                Message::TaskFinished(task) => {
                    println!("Reactive thread received task finished");
                    task_states.set(task, true);
                    for wake_data in wait_data_vec.iter() {
                        if task_states.check_batch(wake_data.handle.as_slice()) {
                            println!("Reactive thread waking up thread {}", wake_data.thread_id);
                            thread_wakers[wake_data.thread_id].wake(i32::MAX);
                        }
                    }
                }
                Message::Quit => {
                    println!("Reactive thread received quit");
                    break;
                }
            }
        }
        println!("Reactive thread quitting");
    }
    fn thread_loop(executor: ExecutorPtr, thread_id: usize) {
        THREAD_LOCAL_ID.set(thread_id);
        let local_id = THREAD_LOCAL_ID.get();
        println!("Thread loop {} started", thread_id);
        let executor = unsafe { &*executor.0 };
        let thread_queues = &executor.thread_queues;
        let thread_waker = &executor.thread_wakers[local_id];
        let quit = &executor.quit;
        let sender = &executor.reactive_sender;

        loop {
            // First take tasks from its own queue
            while let Some(task) = thread_queues[local_id].pop() {
                (task.func)(&executor);
                // Notify the reactive thread that the task is finished
                sender
                    .send(Message::TaskFinished(task.id))
                    .expect("Failed to send finished message");
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
            let quit = quit.load(Acquire);
            if thread_queues[local_id].is_empty() && !quit {
                thread_waker.wait();
            }

            // Drain the queue
            if quit {
                println!("Thread loop {} draining", thread_id);
                while let Some(task) = thread_queues[local_id].pop() {
                    (task.func)(&executor);
                }
                println!("Thread {} quit", thread_id);
                break;
            }
        }
    }
    pub fn new(thread_count: usize) -> Box<Self> {
        // -1 one because the main thread is also a executor thread and not part of the loop
        let threads_to_create = thread_count - 1;
        let mut thread_wakers = Vec::with_capacity(thread_count);
        let (reactive_sender, reactive_receiver) = unbounded::<Message>();

        let mut executor = Box::new(Self {
            thread_queues: Vec::with_capacity(thread_count),
            id_gen: AtomicUsize::new(0),
            reactive_thread_handle: None,
            join_handles: Vec::with_capacity(thread_count),
            thread_wakers,
            quit: AtomicBool::new(false),
            reactive_sender,
        });

        for _ in 0..thread_count {
            executor.thread_wakers.push(Waker::new());
        }

        for _ in 0..thread_count {
            executor.thread_queues.push(ArrayQueue::new(Self::TASK_COUNT));
        }

        // Spin up the executor threads
        unsafe {
            for i in 0..threads_to_create {
                let executor_ptr = ExecutorPtr(&*executor as *const Executor);
                let id = i + 1;
                executor
                    .join_handles
                    .push(std::thread::spawn(move || Self::thread_loop(executor_ptr, id)));
            }
        }

        // Spin up the reactive thread
        unsafe {
            let executor_ptr = ExecutorPtr(&*executor as *const Executor);
            executor.reactive_thread_handle = Some(std::thread::spawn(|| Self::reactive_thread_loop(executor_ptr, reactive_receiver)));
        }

        executor
    }

    fn wake_all(&self) {
        for waker in self.thread_wakers.iter() {
            waker.wake(THREAD_LOCAL_ID.get() as i32);
        }
    }
    pub fn submit(&self, func: Box<dyn FnOnce(&Executor) + Send>) -> usize {
        let generated_id = self.id_gen.load(Acquire);
        println!("Submitting task {} to thread {}", generated_id, THREAD_LOCAL_ID.get());
        self.thread_queues[THREAD_LOCAL_ID.get()].push(Task::new(func, generated_id)).ok();

        // So parking threads will wake up and try to steal
        self.wake_all();

        self.id_gen.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        generated_id
    }

    pub fn yield_until(&self, handles: Vec<Handle>) {
        let thread_queues = &self.thread_queues;
        let waker = &self.thread_wakers[THREAD_LOCAL_ID.get()];
        let local_id = THREAD_LOCAL_ID.get();
        // Notify the reactive thread that it should wake us up when the handles are done
        self.reactive_sender
            .send(Message::WakeOnHandleComplete(WakeData {
                handle: handles,
                thread_id: local_id,
            }))
            .expect("Failed to send message");

        loop {
            // First take tasks from its own queue
            while let Some(task) = thread_queues[local_id].pop() {
                (task.func)(&self);
                // Notify the reactive thread that the task is finished
                self.reactive_sender
                    .send(Message::TaskFinished(task.id))
                    .expect("Failed to send finished message");

                // If reactive thread woke us up, that means our tasks are finished and we can return execution to the user
                if waker.wake_src() == i32::MAX {
                    break;
                }
            }

            // Then take tasks from other queues
            for queue in thread_queues.iter() {
                if let Some(task) = queue.pop() {
                    println!("Thread {} stealing task {} to queue", local_id, task.id);
                    if let Err(_) = thread_queues[local_id].push(task) {
                        panic!("Failed to push to local queue");
                    }
                }
            }

            // Park the thread until woken up by finished task or new task.
            if thread_queues[local_id].is_empty() {
                if waker.wait() == i32::MAX {
                    break;
                }
            }
        }
    }

    pub fn end(&mut self) {
        // We set the state to quit and wake all parked threads so they can exit
        self.quit.store(true, Release);
        self.wake_all();

        //Now we hand over the current thread to the executor so it can finish whatever work there is and exit
        let executor_ptr = ExecutorPtr(&*self as *const Executor);
        Self::thread_loop(executor_ptr, 0);

        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
        self.reactive_sender.send(Message::Quit).expect("Failed to send quit message");
        self.reactive_thread_handle.take().unwrap().join().unwrap();
    }
}
