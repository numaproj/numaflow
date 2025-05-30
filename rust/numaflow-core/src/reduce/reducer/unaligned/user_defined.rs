pub(crate) mod accumulator;
pub(crate) mod session;

#[derive(Clone)]
pub(crate) enum UserDefinedUnalignedReduce {
    Accumulator(accumulator::UserDefinedAccumulator),
    Session(session::UserDefinedSessionReduce),
}
