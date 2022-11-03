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

package commands

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_Commands(t *testing.T) {
	t.Run("root execute", func(t *testing.T) {
		assert.NotPanics(t, Execute, "help")
	})

	t.Run("test root", func(t *testing.T) {
		b := bytes.NewBufferString("")
		rootCmd.SetOut(b)
		rootCmd.SetArgs([]string{"help"})
		Execute()
		output, _ := io.ReadAll(b)
		assert.Contains(t, string(output), "Available Commands")
	})

	t.Run("ISBSvcBufferCreate", func(t *testing.T) {
		cmd := NewISBSvcBufferCreateCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-buffer-create", cmd.Use)
		assert.Equal(t, "stringToString", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "buffer list should not be empty", err.Error())
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1=bb"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "required environment variable '"+dfv1.EnvPipelineName+"' not defined", err.Error())
		os.Setenv(dfv1.EnvPipelineName, "test-pl")
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
	})

	t.Run("ISBSvcBufferDelete", func(t *testing.T) {
		cmd := NewISBSvcBufferDeleteCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-buffer-delete", cmd.Use)
		assert.Equal(t, "stringToString", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "buffer not supplied", err.Error())
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1=bb"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported isb service type")
	})

	t.Run("ISBSvcBufferValidate", func(t *testing.T) {
		cmd := NewISBSvcBufferValidateCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "isbsvc-buffer-validate", cmd.Use)
		assert.Equal(t, "stringToString", cmd.Flag("buffers").Value.Type())
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "buffer not supplied", err.Error())
		cmd.SetArgs([]string{"--isbsvc-type=nonono", "--buffers=buffer1=bb"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Equal(t, "unsupported isb service type", err.Error())
	})

	t.Run("Controller", func(t *testing.T) {
		cmd := NewControllerCommand()
		assert.Equal(t, "controller", cmd.Use)
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "string", cmd.Flag("managed-namespace").Value.Type())
		assert.Equal(t, "bool", cmd.Flag("namespaced").Value.Type())
	})

	t.Run("BuiltinUDF", func(t *testing.T) {
		cmd := NewBuiltinUDFCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "builtin-udf", cmd.Use)
		assert.Equal(t, "string", cmd.Flag("name").Value.Type())
		assert.Equal(t, "stringSlice", cmd.Flag("args").Value.Type())
		cmd.SetArgs([]string{"--name="})
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function name missing")
	})

	t.Run("processor", func(t *testing.T) {
		cmd := NewProcessorCommand()
		assert.True(t, cmd.HasLocalFlags())
		assert.Equal(t, "processor", cmd.Use)
		assert.Equal(t, "string", cmd.Flag("isbsvc-type").Value.Type())
		assert.Equal(t, "string", cmd.Flag("type").Value.Type())
		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), dfv1.EnvVertexObject+"' not defined")
		os.Setenv(dfv1.EnvVertexObject, "xxxxx")
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode vertex string")
		os.Setenv(dfv1.EnvVertexObject, generateEncodedVertexSpecs())
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), dfv1.EnvPod+"' not defined")
		os.Setenv(dfv1.EnvPod, "xxxxx")
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), dfv1.EnvReplica+"' not defined")
		os.Setenv(dfv1.EnvReplica, "$$$")
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid replica")
		os.Setenv(dfv1.EnvReplica, "2")
		cmd.SetArgs([]string{"--type=nonono"})
		err = cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized processor type")
	})
}

func generateEncodedVertexSpecs() string {
	replicas := int32(1)
	v := &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-ns",
			Name:      "test-name",
		},
		Spec: dfv1.VertexSpec{
			Replicas:     &replicas,
			FromEdges:    []dfv1.Edge{{From: "input", To: "name"}},
			ToEdges:      []dfv1.Edge{{From: "name", To: "output"}},
			PipelineName: "test-pl",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "name",
			},
		},
	}
	vertexBytes, _ := json.Marshal(v)
	return base64.StdEncoding.EncodeToString(vertexBytes)
}
