use async_prototype::executor::Executor;
use std::thread::sleep;
fn async_function(executor: &Executor) {
    println!("Hello, world!");
}

fn async_function2(executor: &Executor) {
    println!("Hello, world 2!");
}

fn async_function_task(executor: &Executor) {
    println!("waiting for 1 second");
    sleep(std::time::Duration::from_secs(1));
    println!("Hello, world 4!");
}

fn async_function_nested(executor: &Executor, param: usize) {
    println!("Hello, world {} !", param);
    executor.submit(Box::new(async_function_task));
    println!("execution continues");
}

fn main() {
    let mut executor = Executor::new(2);
    let param = 5;
    executor.submit(Box::new(async_function));
    executor.submit(Box::new(async_function2));
    executor.submit(Box::new(move |executor: &Executor| async_function_nested(executor, param)));
    executor.close();
}
