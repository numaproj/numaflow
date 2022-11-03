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

package jetstream

import "github.com/nats-io/nats.go"

// JetStreamContext is a proxy struct to nats.JetStreamContext
// The existence of this proxy is to replace underlying nats.JetStreamContext
// with new one after reconnection.
type JetStreamContext struct {
	js nats.JetStreamContext
}

func (jsc *JetStreamContext) CreateKeyValue(cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	return jsc.js.CreateKeyValue(cfg)
}

func (jsc *JetStreamContext) StreamInfo(stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	return jsc.js.StreamInfo(stream, opts...)
}

func (jsc *JetStreamContext) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	return jsc.js.AddStream(cfg, opts...)
}

func (jsc *JetStreamContext) AddConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return jsc.js.AddConsumer(stream, cfg, opts...)
}

func (jsc *JetStreamContext) KeyValue(bucket string) (nats.KeyValue, error) {
	return jsc.js.KeyValue(bucket)
}

func (jsc *JetStreamContext) DeleteStream(name string, opts ...nats.JSOpt) error {
	return jsc.js.DeleteStream(name, opts...)
}

func (jsc *JetStreamContext) DeleteKeyValue(bucket string) error {
	return jsc.js.DeleteKeyValue(bucket)
}

func (jsc *JetStreamContext) ConsumerInfo(stream string, name string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return jsc.js.ConsumerInfo(stream, name, opts...)
}

func (jsc *JetStreamContext) PullSubscribe(subj string, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return jsc.js.PullSubscribe(subj, durable, opts...)
}

func (jsc *JetStreamContext) DeleteConsumer(stream string, consumer string, opts ...nats.JSOpt) error {
	return jsc.js.DeleteConsumer(stream, consumer, opts...)
}

func (jsc *JetStreamContext) PublishMsgAsync(m *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return jsc.js.PublishMsgAsync(m, opts...)
}
