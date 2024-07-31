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

package fs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// tests for encoder and decoder
func TestEncodingAndDecoding(t *testing.T) {
	tempDir := t.TempDir()
	defer func() {
		cleanupDir(tempDir)
	}()

	// create a file to write the encoded messages to
	fp, err := os.OpenFile(tempDir+"/testFile", os.O_CREATE|os.O_WRONLY, 0644)
	assert.NoError(t, err)

	ec := newEncoder()
	if ec == nil {
		t.Errorf("Expected newEncoder() to return a non-nil encoder")
	}

	partitionId := &partition.ID{
		Start: time.UnixMilli(60000).In(location),
		End:   time.UnixMilli(70000).In(location),
		Slot:  "testSlot",
	}

	bytes, err := ec.encodeHeader(partitionId)
	assert.NoError(t, err)

	// write the header
	_, err = fp.Write(bytes)
	assert.NoError(t, err)

	// build test read messages
	readMessages := testutils.BuildTestReadMessages(100, time.UnixMilli(60000), []string{"key1:key2"})
	for _, msg := range readMessages {
		bytes, err = ec.encodeMessage(&msg)
		assert.NoError(t, err)
		_, err = fp.Write(bytes)
		assert.NoError(t, err)
	}

	dMsg := &deletionMessage{
		St:   60000,
		Et:   120000,
		Slot: "testSlot",
		Key:  "key1:key2",
	}

	bytes, err = ec.encodeDeletionMessage(dMsg)
	assert.NoError(t, err)
	_, err = fp.Write(bytes)
	assert.NoError(t, err)
	err = fp.Close()
	assert.NoError(t, err)

	// create a decoder
	dc := newDecoder()
	// open the same file in read mode
	fp, err = os.OpenFile(tempDir+"/testFile", os.O_RDONLY, 0644)
	assert.NoError(t, err)

	pid, err := dc.decodeHeader(fp)
	assert.NoError(t, err)
	assert.Equal(t, partitionId, pid)

	// decode the messages
	for i := 0; i < 100; i++ {
		msg, _, err := dc.decodeMessage(fp)
		assert.NoError(t, err)
		assert.Equal(t, readMessages[i].EventTime.UnixMilli(), msg.EventTime.UnixMilli())
	}

	// decode the deletion message
	dm, _, err := dc.decodeDeletionMessage(fp)
	assert.NoError(t, err)
	assert.Equal(t, dMsg, dm)

}
