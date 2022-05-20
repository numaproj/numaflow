package builtin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetExecutors(t *testing.T) {
	t.Run("test good", func(t *testing.T) {
		builtins := []Builtin{
			{
				Name: "cat",
			},
			{
				Name:   "filter",
				KWArgs: map[string]string{"expression": `json(payload).a=="b"`},
			},
		}
		for _, b := range builtins {
			e, err := b.excutor()
			assert.NoError(t, err)
			assert.NotNil(t, e)
		}
	})

	t.Run("test bad", func(t *testing.T) {
		b := &Builtin{
			Name: "catt",
		}
		_, err := b.excutor()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized function")
	})
}

func Test_Start(t *testing.T) {

}
