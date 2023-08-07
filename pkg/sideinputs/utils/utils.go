package utils

import (
	"fmt"
	"os"
)

// CheckFileExists checks if a file with the given fileName exists in the file system.
func CheckFileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	// check if err is "file not exists"
	return !os.IsNotExist(err)
}

// UpdateSideInputFile writes the given value to the Side input file specified.
func UpdateSideInputFile(filePath string, value []byte) error {
	// If the file does not exist, create a new file
	if !CheckFileExists(filePath) {
		f, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create Side Input file%s : %w", filePath, err)
		}
		err = f.Close()
		if err != nil {
			return err
		}
	}
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create Side input file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(value)
	if err != nil {
		return fmt.Errorf("failed to write Side Input file %s : %w", filePath, err)
	}
	return nil
}

// FetchSideInputFile reads a given file and returns the value in bytes
// Used as utility for unit tests
func FetchSideInputFile(filePath string) ([]byte, error) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Side Input %s file: %w", filePath, err)
	}
	return b, nil
}
