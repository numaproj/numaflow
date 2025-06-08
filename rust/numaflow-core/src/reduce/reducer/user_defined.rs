//! User-defined Reduce for Aligned and Unaligned Windows.

use crate::reduce::reducer::aligned::user_defined as aligned;
use crate::reduce::reducer::unaligned::user_defined as unaligned;

/// Enum to represent either aligned or unaligned user defined clients.
#[derive(Clone)]
pub(crate) enum UserDefinedReduce {
    Aligned(aligned::UserDefinedAlignedReduce),
    Unaligned(unaligned::UserDefinedUnalignedReduce),
}
