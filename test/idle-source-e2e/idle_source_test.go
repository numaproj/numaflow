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

/* These tests will test the functionality of progressing watermark in case of idle, while publishing the data to only one
replica instead of two. Once "threshold" reached to 5s(configurable) and if source is found
to be idle then it will increment the watermark by 3s(configurable) after waiting for stepInterval of 2s(configurable).
*/

//go:generate kubectl -n numaflow-system delete statefulset zookeeper kafka-broker --ignore-not-found=true
//go:generate kubectl apply -k ./kafka -n numaflow-system
// Wait for zookeeper to come up
//go:generate sleep 60

package idle_source_e2e

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type IdleSourceSuite struct {
	E2ESuite
}

func (is *IdleSourceSuite) TestIdleKeyedReducePipelineWithHttpSource() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := is.Given().Pipeline("@testdata/idle-source-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "idle-source"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	done := make(chan struct{})
	go func() {
		// publish messages to source vertex, with event time starting from 1000
		startTime := 1000
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				eventTime := strconv.Itoa(startTime + i*1000)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// since the key can be even or odd and the window duration is 10s
	// the sum should be 20(for even) and 40(for odd)
	w.Expect().
		SinkContains("sink", "40", WithTimeout(120*time.Second)).
		SinkContains("sink", "20", WithTimeout(120*time.Second))
	done <- struct{}{}
}

func (is *IdleSourceSuite) TestIdleKeyedReducePipelineWithKafkaSource() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	inputTopic := "input-topic"
	fileData, err := os.ReadFile("testdata/kafka-pipeline.yaml")
	is.NoError(err)
	updatedFileData := strings.ReplaceAll(string(fileData), "my-topic", inputTopic)

	w := is.Given().Pipeline(updatedFileData).When().CreatePipelineAndWait()
	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	defer w.DeletePipelineAndWait()
	defer DeleteKafkaTopic(inputTopic)

	done := make(chan struct{})
	go func() {
		startTime := time.Now().Add(-time.Second * 10)
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				SendMessage(inputTopic, "data", generateMsg("1", startTime))
				time.Sleep(10 * time.Millisecond)
				startTime = startTime.Add(1 * time.Second)
			}
		}
	}()

	ExpectKafkaTopicCount(inputTopic, 15, 3*time.Second)
	// since the window duration is 10 second, so the count of event will be 10
	w.Expect().SinkContains("sink", "10", WithTimeout(120*time.Second))
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

func TestReduceSuite(t *testing.T) {
	suite.Run(t, new(IdleSourceSuite))
}
