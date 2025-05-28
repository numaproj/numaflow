use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::{UnalignedWindowMessage, Window};
use chrono::{DateTime, Utc};

/// SessionWindowManager manages session windows.
#[derive(Debug, Clone)]
pub(crate) struct SessionWindowManager {}

impl SessionWindowManager {
    pub(crate) fn new(_timeout: Duration) -> Self {
        Self {}
    }

    pub(crate) fn assign_windows(&self, _msg: Message) -> Vec<UnalignedWindowMessage> {
        unimplemented!()
    }

    pub(crate) fn close_windows(&self, _watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        unimplemented!()
    }

    pub(crate) fn delete_closed_window(&self, _window: Window) {
        unimplemented!()
    }

    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        unimplemented!()
    }
}
