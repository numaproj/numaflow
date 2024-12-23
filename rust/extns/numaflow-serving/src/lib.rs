mod source;
pub use source::{Message, MessageWrapper, ServingSource};

mod app;
pub mod config;

mod errors;
pub use errors::{Error, Result};

pub(crate) mod pipeline;

pub type Settings = config::Settings;
