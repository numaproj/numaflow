package unaligned

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

const output = "this is just an example string"

func BenchmarkBufioWriter(b *testing.B) {
	file, _ := os.Create("test.txt")
	defer file.Close()
	buffer := bufio.NewWriter(file)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.Write([]byte(output + fmt.Sprintf("%d\n", i)))
		buffer.Flush()
	}
}

// BenchmarkBufioWriter-10    	  752877	      1618 ns/op	      48 B/op	       1 allocs/op - message - "yashash"
// BenchmarkBufioWriter-10    	  677716	      1781 ns/op	     126 B/op	       3 allocs/op - message - "yashash - %d\n"

func BenchmarkFileWriteAt(b *testing.B) {
	file, _ := os.Create("test.txt")
	defer file.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file.WriteAt([]byte(output+fmt.Sprintf("%d\n", i)), int64(i))
	}
}

// BenchmarkFileWriteAt-10    	  636252	      1646 ns/op	       0 B/op	       0 allocs/op - message - "yashash"
// BenchmarkFileWriteAt-10    	  675021	      1686 ns/op	     126 B/op	       3 allocs/op - message - "yashash - %d\n"
