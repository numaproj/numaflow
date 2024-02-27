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
	"sort"
	"strconv"
	"strings"
)

// filesInDir lists all filesToReplay after applying the filterStr in the given directory,
// except the WIP temp file.
func filesInDir(dirPath string, filterStr string) ([]os.FileInfo, error) {

	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func(dir *os.File) {
		_ = dir.Close()
	}(dir)

	// read all filesToReplay from the dir
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var cfs []os.FileInfo
	// ignore the filesToReplay which has "current" in their file name
	// because it will in use by the writer
	for i := 0; i < len(files); i++ {
		if strings.Contains(files[i].Name(), filterStr) {
			continue
		}
		cfs = append(cfs, files[i])
	}

	return cfs, nil
}

// listFilesInDir lists all filesToReplay after applying the filterStr in the given directory. If a sort function is provided,
// the filesToReplay are sorted using it.
func listFilesInDir(dirPath, filterStr string, sortFunc func([]os.FileInfo)) ([]os.FileInfo, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func(dir *os.File) {
		_ = dir.Close()
	}(dir)

	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// filter the filesToReplay based on the filterStr
	var cfs []os.FileInfo
	for _, file := range files {
		if strings.Contains(file.Name(), filterStr) {
			continue
		}
		cfs = append(cfs, file)
	}

	// sort the filesToReplay if a sort function is provided
	if sortFunc != nil {
		sortFunc(cfs)
	}

	return cfs, nil
}

// sortFunc is a function to sort the filesToReplay based on the timestamp in the file name
var sortFunc = func(files []os.FileInfo) {
	sort.Slice(files, func(i, j int) bool {
		timeI, _ := strconv.ParseInt(strings.Split(files[i].Name(), "-")[1], 10, 64)
		timeJ, _ := strconv.ParseInt(strings.Split(files[j].Name(), "-")[1], 10, 64)
		return timeI < timeJ
	})
}
