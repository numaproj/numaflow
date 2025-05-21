//! User-defined Reduce for Aligned and Unaligned Windows.

use crate::reduce::reducer::aligned::user_defined as aligned;
use crate::reduce::reducer::unaligned::user_defined as unaligned;

#[derive(Clone)]
pub(crate) enum ReduceClient {
    Fixed(aligned::UserDefinedAlignedReduce),
    Sliding(aligned::UserDefinedAlignedReduce),
    Session(unaligned::session::UserDefinedSessionReduce),
    Accumulator(unaligned::accumulator::UserDefinedAccumulator),
}
