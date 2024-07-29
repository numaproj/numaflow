package isb

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkStringSprintf(b *testing.B) {
	id := MessageID{
		VertexName: "vertex",
		Offset:     "offset",
		Index:      1,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%s-%s-%d", id.VertexName, id.Offset, id.Index)
	}
}

func BenchmarkStringConcat(b *testing.B) {
	id := MessageID{
		VertexName: "vertex",
		Offset:     "offset",
		Index:      1,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = id.VertexName + "-" + id.Offset + "-" + strconv.Itoa(int(id.Index))
	}
}
