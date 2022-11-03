//go:build isb_jetstream

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

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
)

var natsJetStreamUrl = "nats://localhost:4222"

func TestJetStreamBufferRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "testJetStreamBufferReader"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bw, err := NewJetStreamBufferWriter(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)
	jw, _ := bw.(*jetStreamWriter)
	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(20), startTime)
	// Verify if buffer is full.
	for jw.isFull.Load() {
		select {
		case <-ctx.Done():
			t.Fatalf("expected not to be full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	// Test Write
	_, errs := jw.Write(ctx, messages)
	assert.Equal(t, len(errs), 20)

	bufferReader, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)

	fromStep := bufferReader.(*jetStreamReader)

	readMessages, err := fromStep.Read(ctx, 20)
	assert.NoError(t, err)
	offsetsInsideReadMessages := make([]isb.Offset, len(messages))
	messagesInsideReadMessages := make([]isb.Message, len(messages))
	for idx, readMessage := range readMessages {
		messagesInsideReadMessages[idx] = readMessage.Message
		offsetsInsideReadMessages[idx] = readMessage.ReadOffset
	}

	assert.Equal(t, 20, len(readMessages))

	fromStepJs, err := fromStep.conn.JetStream()
	assert.NoError(t, err)
	streamInfo, err := fromStepJs.StreamInfo(streamName)
	assert.NoError(t, err)
	// Un-acked messages should be number of read messages
	assert.Equal(t, uint64(20), streamInfo.State.Msgs)

	errs = fromStep.Ack(ctx, offsetsInsideReadMessages)
	for _, e := range errs {
		assert.NoError(t, e)
	}

	// Verify if number of messages turns to 0 after acking
	for streamInfo.State.Msgs != 0 {
		streamInfo, _ = fromStepJs.StreamInfo(streamName)
		if streamInfo.State.Msgs == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("not 0 msgs, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// After acking messages number of messages should be 0
	assert.Equal(t, 0, streamInfo.State.Msgs)

}

// TestGetName is used to test the GetName function
func TestGetName(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "getName"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bufferReader, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)
	br := bufferReader.(*jetStreamReader)
	assert.Equal(t, br.GetName(), streamName)

}

// TestClose is used to test Close
func TestClose(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	opts := nats.UserInfo("", "")
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(natsJetStreamUrl, opts)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	streamName := "close"
	addStream(t, js, streamName)
	defer deleteStream(js, streamName)

	bufferReader, err := NewJetStreamBufferReader(ctx, defaultJetStreamClient, streamName, streamName, streamName)
	assert.NoError(t, err)

	br := bufferReader.(*jetStreamReader)
	assert.NoError(t, br.Close())

}

// TestConvert2IsbMsgHeader is used to convert isb header
func TestConvert2IsbMsgHeader(t *testing.T) {
	natsHeader := nats.Header{}
	natsHeader.Set("w", "1")
	natsHeader.Set("ps", "1636470000")
	natsHeader.Set("pen", "1636470060")

	assert.NotNil(t, convert2IsbMsgHeader(natsHeader))
}

func addStream(t *testing.T, js *jsclient.JetStreamContext, streamName string) {
	_, err := js.AddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{streamName},
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardOld,
		MaxMsgs:    100, //
		Storage:    nats.FileStorage,
		Duplicates: 2 * 60 * time.Second,
	})
	assert.NoError(t, err)

	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       streamName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       2 * time.Second,
		FilterSubject: streamName,
	})
	assert.NoError(t, err)

}

func deleteStream(js *jsclient.JetStreamContext, streamName string) {
	_ = js.DeleteConsumer(streamName, streamName)
	_ = js.DeleteStream(streamName)
}
