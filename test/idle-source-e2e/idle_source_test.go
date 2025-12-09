//go:build test

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

/* Test the functionality of progressing watermark in case of idling.
for example: publishing the data to only one replica instead of multiples.
Once "threshold" reached to 5s(configurable) and if source is found as idle, then it will increment the watermark by
3s(configurable) after waiting for stepInterval of 2s(configurable).
*/

package idle_source_e2e

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete statefulset zookeeper kafka-broker --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/kafka -n numaflow-system

type IdleSourceSuite struct {
	E2ESuite
}

func (is *IdleSourceSuite) TestIdleKeyedReducePipelineWithHttpSource() {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	w := is.Given().Pipeline("@testdata/idle-source-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "http-idle-source"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	done := make(chan struct{})
	go func() {
		// publish messages to source vertex, with event time starting from 0
		startTime := int(time.Now().UnixMilli())
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				startTime = startTime + 100
				eventTime := strconv.Itoa(startTime)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// since the key can be even or odd and the window duration is 10s
	// the sum should be 20(for even) and 40(for odd)
	w.Expect().
		RedisSinkContains("http-idle-source-sink", "200", SinkCheckWithTimeout(300*time.Second)).
		RedisSinkContains("http-idle-source-sink", "400", SinkCheckWithTimeout(300*time.Second))
	done <- struct{}{}
}

func (is *IdleSourceSuite) TestIdleKeyedReducePipelineWithKafkaSource() {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// create kafka topic with 2 partitions
	topic := "kafka-topic"
	CreateKafkaTopic(topic, 2)

	w := is.Given().Pipeline("@testdata/kafka-pipeline.yaml").When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	defer DeleteKafkaTopic(topic)

	done := make(chan struct{})
	go func() {
		startTime := time.UnixMilli(60000)
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				// send message to both partition for first 600 messages for overcome the kafka source lazy loading wm publisher.
				// after that send message to only one partition. so that idle source will be detected and wm will be progressed.
				SendMessage(topic, "data", generateMsg("1", startTime), 0)
				if i < 600 {
					SendMessage(topic, "data", generateMsg("2", startTime), 1)
				}
				time.Sleep(10 * time.Millisecond)
				startTime = startTime.Add(1 * time.Second)
			}
		}
	}()

	// since the window duration is 10 second, so the count of event will be 20, when sending data to both partitions.
	w.Expect().RedisSinkContains("kafka-idle-source-sink", "20", SinkCheckWithTimeout(300*time.Second))
	// since the window duration is 10 second, so the count of event will be 10, when sending data to only one partition.
	w.Expect().RedisSinkContains("kafka-idle-source-sink", "10", SinkCheckWithTimeout(300*time.Second))

	done <- struct{}{}
}

type data struct {
	Value string    `json:"value"`
	Time  time.Time `json:"time"`
}

func generateMsg(msg string, t time.Time) string {
	testMsg := data{Value: msg, Time: t}
	jsonBytes, err := json.Marshal(testMsg)
	if err != nil {
		log.Fatalf("failed to marshal test message: %v", err)
	}
	return string(jsonBytes)
}

func TestIdleSourceSuite(t *testing.T) {
	suite.Run(t, new(IdleSourceSuite))
}
