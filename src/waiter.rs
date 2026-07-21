use crossbeam_utils::Backoff;
use std::time::Duration;

pub(crate) struct Waiter {
    pub(crate) sleep_duration: Duration,
}

impl Waiter {
    #[allow(unused)]
    pub(crate) fn spin_wait(&self, mut f: impl FnMut() -> bool) {
        let b = Backoff::new();
        loop {
            if f() {
                return;
            }
            if b.is_completed() {
                std::thread::sleep(self.sleep_duration);
            } else {
                b.spin();
            }
        }
    }

    pub(crate) fn snooze_wait(&self, mut f: impl FnMut() -> bool) {
        let b = Backoff::new();

        loop {
            if f() {
                return;
            }
            if b.is_completed() {
                std::thread::sleep(self.sleep_duration);
            } else {
                b.snooze();
            }
        }
    }
}
