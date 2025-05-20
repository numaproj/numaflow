//! User-defined Reduce for Aligned and Unaligned Windows.

use crate::reduce::reducer::aligned::user_defined;

#[derive(Clone)]
pub(crate) enum ReduceClient {
    Aligned(user_defined::UserDefinedAlignedReduce),
}
