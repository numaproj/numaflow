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
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// CheckFileExists checks if a file with the given fileName exists in the file system.
func CheckFileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	// check if err is "file not exists"
	return !os.IsNotExist(err)
}

// UpdateSideInputFile writes the given side input value to a new file
// and updates the side input store path to point to this new file.
func UpdateSideInputFile(ctx context.Context, fileSymLink string, value []byte) error {
	log := logging.FromContext(ctx)
	// Generate a new file name using timestamp
	timestamp := time.Now().UnixNano()
	newFileName := fmt.Sprintf("%s_%d", fileSymLink, timestamp)

	// Fetch the current side input value from the file
	currentValue, err := FetchSideInputFileValue(fileSymLink)

	// Check if the current value is same as the new value
	// If true then don't update file again and return
	if err == nil && bytes.Equal(currentValue, value) {
		log.Debugw("Side Input value is same as current value, "+
			"skipping update", zap.String("side_input", fileSymLink))
		return nil
	}

	// Write the side input value to the new file
	// A New file is created with the given name if it doesn't exist
	err = os.WriteFile(newFileName, value, 0666)
	if err != nil {
		return fmt.Errorf("failed to write Side Input file %s : %w", newFileName, err)
	}

	// Get the old file path
	oldFilePath, _ := os.Readlink(fileSymLink)

	// Create a temp symlink to the new file. This is done to have an atomic way of
	// updating the side input store path.
	symlinkPathTmp := fmt.Sprintf("%s_%s_%d", fileSymLink, "temp", timestamp)

	// Create a temp symlink to the new file
	if err := os.Symlink(newFileName, symlinkPathTmp); err != nil {
		return err
	}

	// Update the symlink to point to the new file
	err = os.Rename(symlinkPathTmp, fileSymLink)
	if err != nil {
		return fmt.Errorf("failed to update symlink for Side Input file %s : %w", newFileName, err)
	}

	// Remove the old file
	if CheckFileExists(oldFilePath) {
		err = os.Remove(oldFilePath)
		if err != nil {
			log.Errorw("Failed to remove old Side Input file %s : %w", oldFilePath, err)
		}
	}
	return nil
}

// FetchSideInputFileValue reads a given file and returns the value in bytes
func FetchSideInputFileValue(filePath string) ([]byte, error) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Side Input %s file: %w", filePath, err)
	}
	return b, nil
}
