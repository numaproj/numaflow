pub(crate) mod aligned;
pub(crate) mod unaligned;

#[derive(Clone)]
pub(crate) enum ReduceClient {
    Aligned(aligned::UserDefinedAlignedReduce),
    Unaligned(unaligned::UserDefinedUnalignedReduce),
}
