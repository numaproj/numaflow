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

package utils

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Delete mountPath directory if it exists
func cleanup(mountPath string) {
	if CheckFileExists(mountPath) {
		_ = os.RemoveAll(mountPath)
	}
}

// TestSymLinkUpdate tests that the symlink is updated with a new file
// whenever data is written to the symlink.
func TestSymLinkUpdate(t *testing.T) {
	mountPath, err := os.MkdirTemp("", "side-input")
	assert.NoError(t, err)
	// Clean up
	defer cleanup(mountPath)

	fileName, err := os.CreateTemp(mountPath, "unit-test")
	assert.NoError(t, err)
	filePath := fileName.Name()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if !CheckFileExists(mountPath) {
		err := os.Mkdir(mountPath, 0777)
		assert.NoError(t, err)
	}
	byteArray := []byte("test")
	// Write data to the link
	err = UpdateSideInputFile(ctx, filePath, byteArray)
	assert.NoError(t, err)
	// Get the target file from the symlink
	file1, err := os.Readlink(filePath)
	assert.NoError(t, err)
	byteArray = []byte("test-new")
	// Write data to the link again with new value
	err = UpdateSideInputFile(ctx, filePath, byteArray)
	assert.NoError(t, err)
	// Get the new target file from the symlink
	file2, err := os.Readlink(filePath)
	assert.NoError(t, err)
	// We expect the target to be different
	assert.NotEqual(t, file1, file2)
}

// TestSymLinkFileDelete tests that when symlink is updated with a new file
// the older file is deleted.
func TestSymLinkFileDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mountPath, err := os.MkdirTemp("", "side-input")
	assert.NoError(t, err)
	// Clean up
	defer cleanup(mountPath)

	filePath, err := os.CreateTemp(mountPath, "unit-test")
	assert.NoError(t, err)
	fileName := filePath.Name()

	// Write data to the link
	byteArray := []byte("test")
	err = UpdateSideInputFile(ctx, fileName, byteArray)
	assert.NoError(t, err)
	// Get the target file from the symlink
	file1, err := os.Readlink(fileName)
	assert.NoError(t, err)
	// Write data to the link again
	byteArray = []byte("test-new")
	err = UpdateSideInputFile(ctx, fileName, byteArray)
	assert.NoError(t, err)
	// The older file should have been deleted
	assert.False(t, CheckFileExists(file1))
}

// TestUpdateSideInputFileNoUpdate tests if the new value is same as the current
// value then new file isn't created and file is not updated.
func TestUpdateSideInputFileNoUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mountPath, err := os.MkdirTemp("", "side-input")
	assert.NoError(t, err)
	// Clean up
	defer cleanup(mountPath)

	filePath, err := os.CreateTemp(mountPath, "unit-test")
	assert.NoError(t, err)
	fileName := filePath.Name()

	byteArray := []byte("test")
	// Write data to the link
	err = UpdateSideInputFile(ctx, fileName, byteArray)
	assert.NoError(t, err)
	// Get the target file from the symlink
	file1, err := os.Readlink(fileName)
	assert.NoError(t, err)
	data1, err := FetchSideInputFileValue(fileName)
	assert.NoError(t, err)
	// Write data to the link again with same value
	err = UpdateSideInputFile(ctx, fileName, byteArray)
	assert.NoError(t, err)
	// Get the new target file from the symlink
	file2, err := os.Readlink(fileName)
	assert.NoError(t, err)
	data2, err := FetchSideInputFileValue(fileName)
	assert.NoError(t, err)
	// We expect the target to be same file
	assert.Equal(t, file1, file2)
	// We expect the target to have the same data
	assert.True(t, bytes.Equal(data1, data2))

}
