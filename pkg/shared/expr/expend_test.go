package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpand(t *testing.T) {
	m := map[string]interface{}{
		"name": "test",
		"a":    "2",
		"a.b":  "3",
		"a.c":  "4",
	}
	m1 := Expand(m)
	assert.IsType(t, m1["a"], m1)
	assert.Len(t, m1["a"], 2)
	assert.Equal(t, "test", m1["name"])
	c1 := m1["a"].(map[string]interface{})
	assert.Equal(t, "3", c1["b"])
	assert.Equal(t, "4", c1["c"])
}
