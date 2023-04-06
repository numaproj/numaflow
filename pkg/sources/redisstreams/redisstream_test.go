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

	"github.com/stretchr/testify/assert"
)

func TestMsgIdToTime(t *testing.T) {
	tests := []struct {
		testCase     string
		id           string
		expectedTime time.Time
		expectedErr  bool
	}{
		{
			testCase:     "Valid",
			id:           "1518951480106-0",
			expectedTime: time.Date(2018, 2, 18, 2, 58, 0, 106000000, time.Local),
			expectedErr:  false,
		},
		{
			testCase:     "InvalidFormat",
			id:           "1518951480106",
			expectedTime: time.Time{},
			expectedErr:  true,
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
