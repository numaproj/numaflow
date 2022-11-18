package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/stretchr/testify/assert"
)

func Test_writeReadHeader(t *testing.T) {
	id := &partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Key:   "test1",
	}

	tmp := t.TempDir()
	opts := &store.StoreOptions{}
	err := store.WithStorePath(tmp)(opts)
	assert.NoError(t, err)

	wal, err := NewWAL(context.Background(), id, opts)
	fName := wal.fp.Name()
	assert.NoError(t, err)
	// read will fail because the file was opened only in write only mode
	_, err = wal.readHeader()
	assert.Error(t, err)
	err = wal.Close()
	fmt.Println(fName)
	assert.NoError(t, err)

	openWAL, err := OpenWAL(fName)
	assert.NoError(t, err)
	// we have already read the header in OpenWAL
	_, err = openWAL.readHeader()
	assert.Error(t, err)

	// compare the original ID with read ID
	assert.Equal(t, id, openWAL.partitionID)
}

func Test_encodeDecodeHeader(t *testing.T) {
	tests := []struct {
		name    string
		id      *partition.ID
		want    *bytes.Buffer
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "enc_dec_good",
			wantErr: assert.NoError,
			id: &partition.ID{
				Start: time.Unix(1665109020, 0).In(location),
				End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
				Key:   "test1,test2",
			},
		},
		{
			name:    "enc_dec_nodata",
			wantErr: assert.NoError,
			id: &partition.ID{
				Start: time.Time{},
				End:   time.Time{},
				Key:   "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeHeader(tt.id)
			if !tt.wantErr(t, err, fmt.Sprintf("encodeHeader(%v)", tt.id)) {
				return
			}
			result, err := decodeHeader(got)
			assert.NoError(t, err)
			assert.Equalf(t, tt.id, result, "encodeHeader(%v)", tt.id)
		})
	}
}

func Test_runeEncoding(t *testing.T) {
	var inputs = [][]rune{
		{'世', '界'},
		{'1', '2'},
		{'g', 'o', 'o', 'd', ' ', 'b', 'y', 'e'},
	}
	for _, input := range inputs {
		var err error
		// write
		var buf = new(bytes.Buffer)
		for _, runeValue := range input {
			err := binary.Write(buf, binary.LittleEndian, runeValue)
			assert.NoError(t, err)
		}

		// read
		var output = make([]rune, len(input))
		err = binary.Read(buf, binary.LittleEndian, &output)
		assert.NoError(t, err)

		assert.NotEmpty(t, output[0]) // shouldn't be 0 at any cost
		assert.Equal(t, input, output)
	}
}

func Test_writeReadEntry(t *testing.T) {
	id := &partition.ID{
		Start: time.Unix(1665109020, 0).In(location),
		End:   time.Unix(1665109020, 0).Add(time.Minute).In(location),
		Key:   "test1",
	}

	tmp := t.TempDir()
	opts := &store.StoreOptions{}
	err := store.WithStorePath(tmp)(opts)
	assert.NoError(t, err)

	wal, err := NewWAL(context.Background(), id, opts)
	assert.NoError(t, err)

	startTime := time.Unix(1665109020, 0).In(location)
	msgCount := 2
	writeMessages := testutils.BuildTestReadMessagesIntOffset(int64(msgCount), startTime)
	message := writeMessages[0]
	err = wal.Write(&message)
	assert.NoError(t, err)
	err = wal.Close()
	assert.NoError(t, err)

	// Reopen the WAL for read and write.
	wal, err = NewWAL(context.Background(), id, opts)
	assert.NoError(t, err)
	// we have already read the header in OpenWAL
	_, err = wal.readHeader()
	assert.Error(t, err)

	actualMessages, finished, err := wal.Read(10000)
	assert.NoError(t, err)
	// Check we reach the end of file
	expectedEOF := true
	assert.Equal(t, expectedEOF, finished)
	assert.Equal(t, wal.readUpTo, wal.rOffset)

	assert.Len(t, actualMessages, 1)
	actualMessage := actualMessages[0]
	assert.Equalf(t, message.Message, actualMessage.Message, "Read(%v)", message.Message)
	expectedOffset, err := message.ReadOffset.Sequence()
	assert.NoError(t, err)
	actualOffset, err := actualMessage.ReadOffset.Sequence()
	assert.NoError(t, err)
	assert.Equalf(t, expectedOffset, actualOffset, "Read(%v)", message.ReadOffset)
	assert.Equalf(t, message.Watermark, actualMessage.Watermark, "encodeEntry(%v)", message.Watermark)

	// Start to write an entry again
	err = wal.Write(&message)
	assert.NoError(t, err)
	err = wal.Close()
	assert.NoError(t, err)
}

func Test_encodeDecodeEntry(t *testing.T) {
	// write 1 isb messages to persisted store
	startTime := time.Unix(1665109020, 0).In(location)
	writeMessages := testutils.BuildTestReadMessagesIntOffset(1, startTime)
	firstMessage := writeMessages[0]
	tests := []struct {
		name    string
		message *isb.ReadMessage
		want    *bytes.Buffer
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "enc_dec_good",
			wantErr: assert.NoError,
			message: &firstMessage,
		},
		{
			name:    "enc_dec_nodata",
			wantErr: assert.NoError,
			message: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{},
					Body:   isb.Body{},
				},
				ReadOffset: isb.SimpleIntOffset(func() int64 { return int64(2) }),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeEntry(tt.message)
			if !tt.wantErr(t, err, fmt.Sprintf("encodeEntry(%v)", tt.message)) {
				return
			}

			result, _, err := decodeEntry(bytes.NewReader(got.Bytes()))
			assert.NoError(t, err)
			assert.Equalf(t, tt.message.Message, result.Message, "encodeEntry(%v)", tt.message.Message)
			expectedOffset, err := tt.message.ReadOffset.Sequence()
			assert.NoError(t, err)
			actualOffset, err := result.ReadOffset.Sequence()
			assert.NoError(t, err)
			assert.Equalf(t, expectedOffset, actualOffset, "encodeEntry(%v)", tt.message.ReadOffset)
			assert.Equalf(t, tt.message.Watermark, result.Watermark, "encodeEntry(%v)", tt.message.Watermark)
		})
	}
}
