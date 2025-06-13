/// User defined client for accumulator reduce
pub(crate) mod accumulator;

/// User defined client for session reduce
pub(crate) mod session;

/// User defined client for unaligned reduce
#[derive(Clone)]
pub(crate) enum UserDefinedUnalignedReduce {
    Accumulator(accumulator::UserDefinedAccumulator),
    Session(session::UserDefinedSessionReduce),
}
