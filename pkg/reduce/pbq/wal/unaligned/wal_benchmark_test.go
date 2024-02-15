package unaligned

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
