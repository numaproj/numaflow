package utils

import (
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
	var (
		mountPath = "/tmp/side-input/"
		filePath  = "/tmp/side-input/unit-test"
		size      = int64(10 * 1024 * 1024) // 100 MB
		byteArray = make([]byte, size)
	)
	defer cleanup(mountPath)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if !CheckFileExists(mountPath) {
		err := os.Mkdir(mountPath, 0777)
		assert.NoError(t, err)
	}
	// Write data to the link
	err := UpdateSideInputFile(ctx, filePath, byteArray)
	assert.NoError(t, err)
	// Get the target file from the symlink
	file1, err := os.Readlink(filePath)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	// Write data to the link again
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
	var (
		mountPath = "/tmp/side-input/"
		filePath  = "/tmp/side-input/unit-test"
		size      = int64(10 * 1024 * 1024) // 100 MB
		byteArray = make([]byte, size)
	)
	defer cleanup(mountPath)
	if !CheckFileExists(mountPath) {
		err := os.Mkdir(mountPath, 0777)
		assert.NoError(t, err)
	}

	// Write data to the link
	err := UpdateSideInputFile(ctx, filePath, byteArray)
	assert.NoError(t, err)
	// Get the target file from the symlink
	file1, err := os.Readlink(filePath)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	// Write data to the link again
	err = UpdateSideInputFile(ctx, filePath, byteArray)
	assert.NoError(t, err)
	// The older file should have been deleted
	assert.False(t, CheckFileExists(file1))
}
