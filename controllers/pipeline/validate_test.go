package pipeline

import (
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

var (
	testPipeline = &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name:   "input",
					Source: &dfv1.Source{},
				},
				{
					Name: "p1",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
			},
		},
	}
)

func TestValidatePipeline(t *testing.T) {
	t.Run("test good pipeline", func(t *testing.T) {
		err := ValidatePipeline(testPipeline)
		assert.NoError(t, err)
	})

	t.Run("test nil pipeline", func(t *testing.T) {
		err := ValidatePipeline(nil)
		assert.Error(t, err)
	})

	t.Run("no type", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "abc"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "it could only be either a source, or a sink, or a UDF")
	})

	t.Run("duplicate vertex", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input", Source: &dfv1.Source{}})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate vertex name")
	})

	t.Run("test readtimeout pipeline", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Limits = &dfv1.PipelineLimits{ReadTimeout: "2s"}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})
	t.Run("test invalid readtimeout pipeline", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Limits = &dfv1.PipelineLimits{ReadTimeout: "2"}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
	})
	t.Run("source and sink spedified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Sink = &dfv1.Sink{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only one of")
	})

	t.Run("udf no image and builtin spedified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Builtin = nil
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either specify a builtin function, or a customized image")
	})

	t.Run("udf both image and builtin spedified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Container = &dfv1.Container{Image: "xxxx"}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not specify both builtin function, and a customized image")
	})

	t.Run("edge - invalid vertex name", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "a", To: "b"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no vertex named")
	})

	t.Run("edge - no from", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both from and to need to be specified")
	})

	t.Run("edge - source as to", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p1", To: "input"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not be define as 'to'")
	})

	t.Run("edge - sink as from", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "output", To: "p1"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not be define as 'from'")
	})

	t.Run("vertex not in edges", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input1", Source: &dfv1.Source{}})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not all the vertex names are defined")
	})

	t.Run("pipeline has not source", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source = nil
		testObj.Spec.Vertices[0].UDF = &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline has no source")
	})

	t.Run("pipeline has not sink", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[2].Sink = nil
		testObj.Spec.Vertices[2].UDF = &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline has no sink")
	})

	t.Run("same from and to", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p1", To: "p1"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "same from and to")
	})

	t.Run("N from -> 1 to", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "input", To: "output"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("good conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{KeyIn: []string{"hello"}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})

	t.Run("bad conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges[0].Conditions = &dfv1.ForwardConditions{KeyIn: []string{"hello"}}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid edge")
	})
}

func TestValidateVertex(t *testing.T) {
	t.Run("bad min", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Scale: dfv1.Scale{
				Min: pointer.Int32(0),
				Max: pointer.Int32(1),
			},
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "greater than 0")
	})

	t.Run("min > max", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Scale: dfv1.Scale{
				Min: pointer.Int32(2),
				Max: pointer.Int32(1),
			},
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "or equal to")
	})
}
