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

package generator

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestRead(t *testing.T) {
	ctx := context.Background()
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memGen",
		},
		Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
				Source: &dfv1.Source{
					Generator: &dfv1.GeneratorSource{},
				},
			},
		},
	}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}

	mGen, err := NewMemGen(ctx, m, WithReadTimeout(3*time.Second))
	assert.NoError(t, err)
	messages, err := mGen.Read(ctx, 5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(messages))
}

func TestTimeForValidTime(t *testing.T) {
	nanotime := time.Now().UnixNano()
	parsedtime := timeFromNanos(nanotime, 0)
	assert.Equal(t, nanotime, parsedtime.UnixNano())
}

func TestTimeForInvalidTime(t *testing.T) {
	nanotime := int64(-1)
	parsedtime := timeFromNanos(nanotime, 0)
	assert.True(t, parsedtime.UnixNano() > 0)
}

// Demonstrates testing when provided an explicit message in ValueBlob
func TestReadProvidedMessageBlob(t *testing.T) {
	ctx := context.Background()

	testVal := []byte("HelloNumaWorld")
	blobStr := base64.StdEncoding.EncodeToString(testVal)

	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memGen",
		},
		Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
				Source: &dfv1.Source{
					Generator: &dfv1.GeneratorSource{
						ValueBlob: &blobStr,
					},
				},
			},
		},
	}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}

	mGen, err := NewMemGen(ctx, m, WithReadTimeout(3*time.Second))
	assert.NoError(t, err)
	messages, err := mGen.Read(ctx, 5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(messages))

	// All messages should have same payload of provided message blob
	for _, msg := range messages {
		assert.Equal(t, testVal, msg.Body.Payload)
	}
}
