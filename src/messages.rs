use crate::executor::WakeData;

pub enum Message {
    TaskFinished(usize),
    WakeOnHandleComplete(WakeData),
    Quit,
}
