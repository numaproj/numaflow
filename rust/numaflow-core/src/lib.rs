extern crate core;

use tracing::error;

pub(crate) use self::error::Result;

mod error;
pub(crate) use crate::error::Error;
pub mod monovertex;
pub use crate::monovertex::mono_vertex;

mod config;

mod message;
mod shared;
mod sink;
mod source;
mod transformer;
