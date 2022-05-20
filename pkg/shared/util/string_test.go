package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	str := RandomString(20)
	assert.Equal(t, 20, len(str))
}

func TestRandomLowercaseString(t *testing.T) {
	str := RandomLowerCaseString(20)
	assert.Equal(t, 20, len(str))
	assert.Equal(t, str, strings.ToLower(str))
}

func TestStringSliceContains(t *testing.T) {
	assert.False(t, StringSliceContains(nil, "b"))
	assert.False(t, StringSliceContains([]string{}, "b"))
	list := []string{"a", "b", "c"}
	assert.True(t, StringSliceContains(list, "b"))
	assert.False(t, StringSliceContains(list, "e"))
}
