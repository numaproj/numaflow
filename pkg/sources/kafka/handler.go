package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
)

// consumerHandler struct
type consumerHandler struct {
	inflightacks chan bool
	ready        chan bool
	readycloser  sync.Once
	messages     chan *sarama.ConsumerMessage
	sess         sarama.ConsumerGroupSession
}

// new handler initializes the channel for passing messages
func newConsumerHandler(readChanSize int) *consumerHandler {
	return &consumerHandler{
		ready:    make(chan bool),
		messages: make(chan *sarama.ConsumerMessage, readChanSize),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumerHandler) Setup(sess sarama.ConsumerGroupSession) error {
	consumer.sess = sess
	consumer.readycloser.Do(func() {
		close(consumer.ready)
	})
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumerHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	// wait for inflight acks to be completed.
	<-consumer.inflightacks
	sess.Commit()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		consumer.messages <- message
	}

	return nil
}
