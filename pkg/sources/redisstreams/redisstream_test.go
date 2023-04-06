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

package redisstreams

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestProduceUnkeyedJSONMsg(t *testing.T) {
	tests := []struct {
		testCase     string
		inMsg        redis.XMessage
		expectedBody string
		expectedTime time.Time
	}{
		{
			"MultiKeyValuePair",
			redis.XMessage{
				ID: "1518951480106-0",
				Values: map[string]interface{}{
					"humidity":    "50",
					"temperature": "60",
				},
			},
			`{"humidity":"50","temperature":"60"}`,
			time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
		},
		{
			"SingleKeyValuePair",
			redis.XMessage{
				ID: "1518951480106-1",
				Values: map[string]interface{}{
					"humidity": "50",
				},
			},
			`{"humidity":"50"}`,
			time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
		},
		{
			"NoKeyValuePairs", // not really a valid Redis Streams message but we can test it anyway
			redis.XMessage{
				ID:     "1518951480106-1",
				Values: map[string]interface{}{},
			},
			`{}`,
			time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
		},
	}

	for _, tt := range tests {
		t.Run(tt.testCase, func(t *testing.T) {
			outMsg, err := produceUnkeyedJSONMsg(tt.inMsg)
			assert.NotNil(t, outMsg)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedBody, string(outMsg.Payload))
			assert.Equal(t, tt.expectedTime, outMsg.EventTime)
		})
	}

}

func TestProduceOneKeyedMsg(t *testing.T) {
	tests := []struct {
		testCase     string
		inMsg        redis.XMessage
		expectedKey  string
		expectedBody string
		expectedTime time.Time
		expectedErr  bool
	}{
		{
			"MultiKeyValuePair",
			redis.XMessage{
				ID: "1518951480106-0",
				Values: map[string]interface{}{
					"humidity":    "50",
					"temperature": "60",
				},
			},
			"",
			"",
			time.Time{},
			true,
		},
		{
			"SingleKeyValuePair",
			redis.XMessage{
				ID: "1518951480106-0",
				Values: map[string]interface{}{
					"humidity": "50",
				},
			},
			"humidity",
			"50",
			time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
			false,
		},
		{
			"NoKeyValuePair",
			redis.XMessage{
				ID:     "1518951480106-0",
				Values: map[string]interface{}{},
			},
			"",
			"",
			time.Time{},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testCase, func(t *testing.T) {
			outMsg, err := produceOneKeyedMsg(tt.inMsg)
			if tt.expectedErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedKey, outMsg.Key)
				assert.Equal(t, tt.expectedBody, string(outMsg.Payload))
				assert.Equal(t, tt.expectedTime, outMsg.EventTime)
			}
		})
	}
}

func TestMsgIdToTime(t *testing.T) {
	tests := []struct {
		testCase     string
		id           string
		expectedTime time.Time
		expectedErr  bool
	}{
		{
			"Valid",
			"1518951480106-0",
			time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
			false,
		},
		{
			"InvalidFormat",
			"1518951480106",
			time.Time{},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testCase, func(t *testing.T) {
			tm, err := msgIdToTime(tt.id)
			assert.Equal(t, tt.expectedTime, tm)
			if tt.expectedErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}

		})
	}
}
