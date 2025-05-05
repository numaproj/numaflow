use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use tonic::transport::Channel;

#[derive(Clone)]
pub(crate) struct UserDefinedUnalignedReduce {
    client: SessionReduceClient<Channel>,
}
