package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustJson(t *testing.T) {
	assert.Equal(t, "1", MustJSON(1))
}

func TestUnJSON(t *testing.T) {
	var in int
	MustUnJSON("1", &in)
	assert.Equal(t, 1, in)
}
