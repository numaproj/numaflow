package kafka

import "strconv"

// kafkaOffset implements isb.Offset
// we need topic information to ack the message
type kafkaOffset struct {
	offset       int64
	partitionIdx int32
	topic        string
}

func (k *kafkaOffset) String() string {
	return k.topic + ":" + strconv.FormatInt(k.offset, 10) + ":" + strconv.Itoa(int(k.partitionIdx))
}

func (k *kafkaOffset) Sequence() (int64, error) {
	return k.offset, nil
}

// AckIt acking is taken care by the consumer group
func (k *kafkaOffset) AckIt() error {
	// NOOP
	return nil
}

func (k *kafkaOffset) NoAck() error {
	return nil
}

func (k *kafkaOffset) PartitionIdx() int32 {
	return k.partitionIdx
}

func (k *kafkaOffset) Topic() string {
	return k.topic
}
