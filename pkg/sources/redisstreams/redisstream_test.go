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

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestProduceMsg(t *testing.T) {
	tests := []struct {
		testCase     string
		inMsg        redis.XMessage
		expectedBody string
		expectedKeys []string // in order
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
			[]string{"humidity", "temperature"},
			time.Date(2018, 2, 18, 10, 58, 0, 106000000, time.UTC),
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
			[]string{"humidity"},
			time.Date(2018, 2, 18, 10, 58, 0, 106000000, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.testCase, func(t *testing.T) {
			outMsg, err := produceMsg(tt.inMsg)
			assert.NotNil(t, outMsg)
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedBody, string(outMsg.Payload))
			for i, key := range tt.expectedKeys {
				assert.Equal(t, key, outMsg.Keys[i])
			}
			assert.Equal(t, tt.expectedTime.Local(), outMsg.EventTime)
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
			time.Date(2018, 2, 18, 10, 58, 0, 106000000, time.UTC),
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
			if tt.expectedErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedTime.Local(), tm)
			}

		})
	}
}
