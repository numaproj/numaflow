use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::UnalignedWindowMessage;
use chrono::{DateTime, Utc};

/// SessionWindowManager manages session windows.
#[derive(Debug, Clone)]
pub(crate) struct SessionWindowManager {}

impl SessionWindowManager {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {}
    }

    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        todo!()
    }

    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        todo!()
    }

    pub(crate) fn delete_closed_windows(&self, watermark: DateTime<Utc>) {
        todo!()
    }

    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        todo!()
    }
}
