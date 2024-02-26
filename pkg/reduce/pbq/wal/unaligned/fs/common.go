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
	"strings"
)

// filesInDir lists all files sorted chronologically after applying the filterStr in the given directory,
// except the WIP temp file.
func filesInDir(dirPath string, filterStr string) ([]os.FileInfo, error) {

	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func(dir *os.File) {
		_ = dir.Close()
	}(dir)

	// read all files from the dir
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var cfs []os.FileInfo
	// ignore the files which has "current" in their file name
	// because it will in use by the writer
	for i := 0; i < len(files); i++ {
		if strings.Contains(files[i].Name(), filterStr) {
			continue
		}
		cfs = append(cfs, files[i])
	}

	// FIXME(WAL): cannot rely on the ModTime()
	// sort the files based on the mod time (only one writer) so that order of compaction is maintained
	// when you have multiple segments to compact we should compact the oldest segment first
	sort.Slice(cfs, func(i, j int) bool {
		return cfs[i].ModTime().Before(cfs[j].ModTime())
	})

	return cfs, nil
}
