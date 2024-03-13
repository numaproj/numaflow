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
	"bufio"
	"fmt"
	"os"
	"testing"
)

const output = "this is just an example string%\n"

func BenchmarkBufioWriter(b *testing.B) {
	file, _ := os.Create("test.txt")
	defer func(file *os.File) {
		_ = file.Close()
		_ = os.Remove("test.txt")
	}(file)
	buffer := bufio.NewWriter(file)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = buffer.Write([]byte(output + fmt.Sprintf("%d\n", i)))
	}
	_ = buffer.Flush()
}

func BenchmarkFileWriteAt(b *testing.B) {
	file, _ := os.Create("test.txt")
	defer func(file *os.File) {
		_ = file.Close()
		_ = os.Remove("test.txt")
	}(file)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = file.WriteAt([]byte(output+fmt.Sprintf("%d\n", i)), int64(i))
	}
}
