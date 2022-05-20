package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookupEnvStringOr(t *testing.T) {
	assert.Equal(t, LookupEnvStringOr("fake_env", "hello"), "hello")
	assert.Equal(t, LookupEnvStringOr("HOME", "#")[0], "/"[0])
}
