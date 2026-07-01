package v1

type PipelineISBStreamsDTO struct {
	Streams []PipelineISBStreamDTO `json:"streams"`
	Errors  []ISBMonitorErrorDTO   `json:"errors,omitempty"`
}

type PipelineISBStreamDTO struct {
	Namespace            string   `json:"namespace"`
	Pipeline             string   `json:"pipeline"`
	Vertex               string   `json:"vertex"`
	From                 string   `json:"from,omitempty"`
	To                   string   `json:"to,omitempty"`
	Partition            int      `json:"partition"`
	Stream               string   `json:"stream"`
	Subjects             []string `json:"subjects,omitempty"`
	Messages             uint64   `json:"messages"`
	Bytes                uint64   `json:"bytes"`
	ConsumerCount        int      `json:"consumerCount"`
	FirstSeq             uint64   `json:"firstSeq"`
	LastSeq              uint64   `json:"lastSeq"`
	FirstTimestamp       string   `json:"firstTimestamp,omitempty"`
	LastTimestamp        string   `json:"lastTimestamp,omitempty"`
	Storage              string   `json:"storage,omitempty"`
	Replicas             int      `json:"replicas,omitempty"`
	Retention            string   `json:"retention,omitempty"`
	MaxMessages          int64    `json:"maxMessages,omitempty"`
	Leader               string   `json:"leader,omitempty"`
	SourcePod            string   `json:"sourcePod,omitempty"`
	CollectedAt          string   `json:"collectedAt,omitempty"`
	Scope                string   `json:"scope"`
	SharedByInboundEdges bool     `json:"sharedByInboundEdges"`
}

type PipelineISBConsumersDTO struct {
	Consumers []PipelineISBConsumerDTO `json:"consumers"`
	Errors    []ISBMonitorErrorDTO     `json:"errors,omitempty"`
}

type PipelineISBConsumerDTO struct {
	Namespace            string   `json:"namespace"`
	Pipeline             string   `json:"pipeline"`
	Vertex               string   `json:"vertex"`
	From                 string   `json:"from,omitempty"`
	To                   string   `json:"to,omitempty"`
	Partition            int      `json:"partition"`
	Stream               string   `json:"stream"`
	Consumer             string   `json:"consumer"`
	Durable              string   `json:"durable,omitempty"`
	FilterSubject        string   `json:"filterSubject,omitempty"`
	FilterSubjects       []string `json:"filterSubjects,omitempty"`
	AckPolicy            string   `json:"ackPolicy,omitempty"`
	DeliverPolicy        string   `json:"deliverPolicy,omitempty"`
	AckWaitSeconds       float64  `json:"ackWaitSeconds,omitempty"`
	MaxAckPending        int      `json:"maxAckPending,omitempty"`
	NumAckPending        int      `json:"numAckPending"`
	NumRedelivered       int      `json:"numRedelivered"`
	NumWaiting           int      `json:"numWaiting"`
	NumPending           uint64   `json:"numPending"`
	DeliveredConsumerSeq uint64   `json:"deliveredConsumerSeq"`
	DeliveredStreamSeq   uint64   `json:"deliveredStreamSeq"`
	AckFloorConsumerSeq  uint64   `json:"ackFloorConsumerSeq"`
	AckFloorStreamSeq    uint64   `json:"ackFloorStreamSeq"`
	Leader               string   `json:"leader,omitempty"`
	SourcePod            string   `json:"sourcePod,omitempty"`
	CollectedAt          string   `json:"collectedAt,omitempty"`
	Scope                string   `json:"scope"`
	SharedByInboundEdges bool     `json:"sharedByInboundEdges"`
}

type PipelineISBKVStoresDTO struct {
	KVStores []PipelineISBKVStoreDTO `json:"kvStores"`
	Errors   []ISBMonitorErrorDTO    `json:"errors,omitempty"`
}

type PipelineISBKVStoreDTO struct {
	Namespace   string  `json:"namespace"`
	Pipeline    string  `json:"pipeline"`
	Scope       string  `json:"scope"`
	Direction   string  `json:"direction,omitempty"`
	Vertex      string  `json:"vertex,omitempty"`
	From        string  `json:"from,omitempty"`
	To          string  `json:"to,omitempty"`
	Bucket      string  `json:"bucket"`
	Stream      string  `json:"stream"`
	Values      int     `json:"values"`
	Bytes       uint64  `json:"bytes"`
	History     int64   `json:"history,omitempty"`
	TTLSeconds  float64 `json:"ttlSeconds,omitempty"`
	Replicas    int     `json:"replicas,omitempty"`
	Storage     string  `json:"storage,omitempty"`
	Leader      string  `json:"leader,omitempty"`
	SourcePod   string  `json:"sourcePod,omitempty"`
	CollectedAt string  `json:"collectedAt,omitempty"`
}
