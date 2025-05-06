use crate::message::Message;
use crate::reduce::pnf::aligned::windower::{AlignedWindowMessage, Window};

#[derive(Debug, Clone)]
pub(crate) struct SlidingWindower {}

impl SlidingWindower {
    pub(crate) fn assign_windows(&self, _msg: Message) -> Vec<AlignedWindowMessage> {
        unimplemented!()
    }

    pub(crate) fn close_windows(&self) -> Vec<AlignedWindowMessage> {
        unimplemented!()
    }

    pub(crate) fn delete_window(&self, _window: Window) {
        unimplemented!()
    }

    pub(crate) fn oldest_window_endtime(&self) -> chrono::DateTime<chrono::Utc> {
        unimplemented!()
    }
}
