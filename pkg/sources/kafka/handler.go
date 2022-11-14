/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
