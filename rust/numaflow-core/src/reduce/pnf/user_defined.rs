use crate::reduce::pnf::aligned::user_defined;

#[derive(Clone)]
pub(crate) enum ReduceClient {
    Aligned(user_defined::UserDefinedAlignedReduce),
}
