use std::{
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, Mutex,
    },
};

#[derive(Clone, Debug)]
pub(super) struct TasksNumberGuard {
    counter: Arc<AtomicUsize>,
    sender: Arc<Mutex<mio::unix::pipe::Sender>>,
}

impl TasksNumberGuard {
    pub(super) fn new(
        counter: Arc<AtomicUsize>,
        sender: Arc<Mutex<mio::unix::pipe::Sender>>,
    ) -> Self {
        Self { counter, sender }
    }
}

impl Drop for TasksNumberGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Relaxed);
        if let Ok(mut sender) = self.sender.try_lock() {
            sender.write(&[1]).ok();
        }
    }
}
