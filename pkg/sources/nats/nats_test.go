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

package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	natslib "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
)

func testVertex(t *testing.T, url, subject, queue string, hostname string, replicaIndex int32) *dfv1.VertexInstance {
	t.Helper()
	v := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			AbstractVertex: dfv1.AbstractVertex{
				Name: "test-v",
				Source: &dfv1.Source{
					Nats: &dfv1.NatsSource{
						URL:     url,
						Subject: subject,
						Queue:   queue,
					},
				},
			},
		},
	}
	vi := &dfv1.VertexInstance{
		Vertex:   v,
		Hostname: hostname,
		Replica:  replicaIndex,
	}
	return vi
}

func newInstance(t *testing.T, vi *dfv1.VertexInstance) (sourcer.SourceReader, error) {
	t.Helper()
	ctx := context.Background()
	return New(ctx, vi, WithReadTimeout(1*time.Second))
}

func Test_Single(t *testing.T) {
	// default read timeout is 1 sec, and smaller values seems to be flaky
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server := natstest.RunNatsServer(t)
	defer server.Shutdown()

	url := "127.0.0.1"
	testSubject := "test-single"
	testQueue := "test-queue-single"
	vi := testVertex(t, url, testSubject, testQueue, "test-host", 0)
	ns, err := newInstance(t, vi)
	assert.NoError(t, err)
	assert.NotNil(t, ns)
	assert.Equal(t, "test-v", ns.GetName())
	defer func() { _ = ns.Close() }()

	nc, err := natslib.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	for i := 0; i < 3; i++ {
		err = nc.Publish(testSubject, []byte(fmt.Sprintf("%d", i)))
		assert.NoError(t, err)
	}

	var msgs []*isb.ReadMessage
	var readMessagesCount int

loop:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
			return
		default:
			msgs, err = ns.Read(ctx, 5)
			assert.NoError(t, err)
			readMessagesCount += len(msgs)
			if readMessagesCount == 3 {
				break loop
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func Test_Multiple(t *testing.T) {
	server := natstest.RunNatsServer(t)
	defer server.Shutdown()

	url := "127.0.0.1"
	testSubject := "test-multiple"
	testQueue := "test-queue-multiple"
	v1 := testVertex(t, url, testSubject, testQueue, "test-host1", 0)
	ns1, err := newInstance(t, v1)
	assert.NoError(t, err)
	assert.NotNil(t, ns1)
	defer func() { _ = ns1.Close() }()

	v2 := testVertex(t, url, testSubject, testQueue, "test-hos2", 1)
	ns2, err := newInstance(t, v2)
	assert.NoError(t, err)
	assert.NotNil(t, ns2)
	defer func() { _ = ns2.Close() }()

	nc, err := natslib.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()
	for i := 0; i < 5; i++ {
		err = nc.Publish(testSubject, []byte(fmt.Sprint(i)))
		assert.NoError(t, err)
	}

	read := 0
	// default read timeout is 1 sec, and smaller values seems to be flaky
	timeout := time.After(30 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("Failed reading expected messages in the time period, only got %d", read)
		default:
			m1, err := ns1.Read(context.Background(), 1)
			assert.NoError(t, err)
			read += len(m1)
			m2, err := ns2.Read(context.Background(), 1)
			assert.NoError(t, err)
			read += len(m2)
			if read == 5 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
