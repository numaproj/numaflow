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

package pipeline

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
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
					Name: "input",
					Source: &dfv1.Source{
						UDTransformer: &dfv1.UDTransformer{
							Builtin: &dfv1.Transformer{Name: "filter"},
						}},
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
			Watermark: dfv1.Watermark{
				Disabled: false,
				MaxDelay: &metav1.Duration{Duration: 5 * time.Second},
			},
		},
	}

	testReducePipeline = &dfv1.Pipeline{
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
						Container: &dfv1.Container{
							Image: "my-image",
						},
						GroupBy: &dfv1.GroupBy{
							Window: dfv1.Window{
								Fixed: &dfv1.FixedWindow{
									Length: &metav1.Duration{
										Duration: 60 * time.Second,
									},
								},
							},
							Storage: &dfv1.PBQStorage{
								PersistentVolumeClaim: &dfv1.PersistenceStrategy{
									StorageClassName: nil,
									AccessMode:       &dfv1.DefaultAccessMode,
									VolumeSize:       &dfv1.DefaultVolumeSize,
								},
							},
						},
					},
				},
				{
					Name:       "p2",
					Partitions: pointer.Int32(2),
					UDF: &dfv1.UDF{
						Container: &dfv1.Container{
							Image: "my-image",
						},
						GroupBy: &dfv1.GroupBy{
							Window: dfv1.Window{
								Fixed: &dfv1.FixedWindow{
									Length: &metav1.Duration{
										Duration: 60 * time.Second,
									},
								},
							},
							Keyed: true,
							Storage: &dfv1.PBQStorage{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
				{
					Name: "p3",
					UDF: &dfv1.UDF{
						Container: &dfv1.Container{
							Image: "my-image",
						},
						GroupBy: &dfv1.GroupBy{
							Window: dfv1.Window{
								Sliding: &dfv1.SlidingWindow{
									Length: &metav1.Duration{
										Duration: time.Duration(60 * time.Second),
									},
									Slide: &metav1.Duration{
										Duration: time.Duration(30 * time.Second),
									},
								},
							},
							Storage: &dfv1.PBQStorage{
								PersistentVolumeClaim: &dfv1.PersistenceStrategy{
									StorageClassName: nil,
									AccessMode:       &dfv1.DefaultAccessMode,
									VolumeSize:       &dfv1.DefaultVolumeSize,
								},
							},
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "p2"},
				{From: "p2", To: "p3"},
				{From: "p3", To: "output"},
			},
		},
	}

	testForestPipeline = &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						UDTransformer: &dfv1.UDTransformer{
							Builtin: &dfv1.Transformer{Name: "filter"},
						}},
				},
				{
					Name: "input-1",
					Source: &dfv1.Source{
						UDTransformer: &dfv1.UDTransformer{
							Builtin: &dfv1.Transformer{Name: "filter"},
						}},
				},
				{
					Name: "p1",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
				},
				{
					Name: "p2",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
				{
					Name: "output-1",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
				{From: "input-1", To: "p2"},
				{From: "p2", To: "output-1"},
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
		testObj := testPipeline.DeepCopy()
		testObj.Name = "invalid.name"
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pipeline name")
	})

	t.Run("test invalid pipeline name", func(t *testing.T) {
		err := ValidatePipeline(nil)
		assert.Error(t, err)
	})

	t.Run("test pipeline name too long", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Name = "very-very-very-loooooooooooooooooooooooooooooooooooog"
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "over the max limit")
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

	t.Run("source and sink specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Sink = &dfv1.Sink{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only one of")
	})

	t.Run("transformer no image and builtin specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source.UDTransformer.Builtin = nil
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either specify a builtin transformer, or a customized image")
	})

	t.Run("transformer both image and builtin specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source.UDTransformer.Container = &dfv1.Container{Image: "xxxx"}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not specify both builtin transformer, and a customized image")
	})

	t.Run("udsource no image specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source.UDSource = &dfv1.UDSource{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "a customized image is required")
		testObj.Spec.Vertices[0].Source.UDSource = &dfv1.UDSource{
			Container: &dfv1.Container{
				Image: "",
			},
		}
		err = ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "a customized image is required")
	})

	t.Run("source vertex having both udsource and built-in source specified ", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source.UDSource = &dfv1.UDSource{Container: &dfv1.Container{Image: "xxxx"}}
		testObj.Spec.Vertices[0].Source.Generator = &dfv1.GeneratorSource{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only one of")
	})

	t.Run("udf no image and builtin specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Builtin = nil
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "either specify a builtin function, or a customized image")
	})

	t.Run("udf both image and builtin specified", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Container = &dfv1.Container{Image: "xxxx"}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not specify both builtin function, and a customized image")
	})

	t.Run("forest - two pipelines with 1 source/sink", func(t *testing.T) {
		testObj := testForestPipeline.DeepCopy()
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pipeline")
	})

	t.Run("forest - second pipeline has no sink", func(t *testing.T) {
		testObj := testForestPipeline.DeepCopy()
		testObj.Spec.Vertices[5].Sink = nil
		testObj.Spec.Vertices[5].UDF = &dfv1.UDF{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid vertex")
	})

	t.Run("forest - two pipelines with multiple sources/sinks", func(t *testing.T) {
		testObj := testForestPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input-2", Source: &dfv1.Source{}})
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "output-2", Sink: &dfv1.Sink{}})
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "input-2", To: "p1"})
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p2", To: "output-2"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pipeline")
	})

	t.Run("forest - pipelines have cycles", func(t *testing.T) {
		testObj := testForestPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p1", To: "p1"})
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p2", To: "p2"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pipeline")
	})

	t.Run("valid pipeline with multiple sinks/sources", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input-1", Source: &dfv1.Source{}})
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "output-1", Sink: &dfv1.Sink{}})
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "input-1", To: "p1"})
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "p1", To: "output-1"})
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
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
		assert.Contains(t, err.Error(), "source must have 0 from edges and at least 1 to edge")
	})

	t.Run("edge - sink as from", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "output", To: "p1"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sink must have 0 to edges and at least 1 from edge")
	})

	t.Run("edge - duplicate", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "input", To: "p1"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot define multiple edges")
	})

	t.Run("UDF not connected to pipeline", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input1", UDF: &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "UDF must have to and from edges")
	})

	t.Run("pipeline has not source", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Source = nil
		testObj.Spec.Vertices[0].UDF = &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "input", To: "input"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline has no source")
	})

	t.Run("pipeline has not sink", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[2].Sink = nil
		testObj.Spec.Vertices[2].UDF = &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "output", To: "output"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline has no sink")
	})

	t.Run("last vertex is not sink", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "bad-output", UDF: &dfv1.UDF{Builtin: &dfv1.Function{Name: "cat"}}})
		testObj.Spec.Edges[1] = dfv1.Edge{From: "p1", To: "bad-output"}
		testObj.Spec.Edges = append(testObj.Spec.Edges, dfv1.Edge{From: "bad-output", To: "p1"})
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sink must have 0 to edges and at least 1 from edge")
	})

	t.Run("or conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		operatorOr := dfv1.LogicOperatorOr
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{Tags: &dfv1.TagConditions{
			Operator: &operatorOr,
			Values:   []string{"hello"}}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})

	t.Run("and conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		operatorAnd := dfv1.LogicOperatorAnd
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{Tags: &dfv1.TagConditions{
			Operator: &operatorAnd,
			Values:   []string{"hello"}}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})

	t.Run("not conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		operatorNot := dfv1.LogicOperatorNot
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{Tags: &dfv1.TagConditions{
			Operator: &operatorNot,
			Values:   []string{"hello"}}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})

	t.Run("no operator conditional forwarding", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{Tags: &dfv1.TagConditions{
			Values: []string{"hello"}}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})

	t.Run("allow conditional forwarding from source vertex", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		operatorOr := dfv1.LogicOperatorOr
		testObj.Spec.Edges[1].Conditions = &dfv1.ForwardConditions{Tags: &dfv1.TagConditions{
			Operator: &operatorOr,
			Values:   []string{"hello"}}}
		err := ValidatePipeline(testObj)
		assert.NoError(t, err)
	})
}

func TestValidateReducePipeline(t *testing.T) {
	t.Run("test good reduce pipeline", func(t *testing.T) {
		err := ValidatePipeline(testReducePipeline)
		assert.NoError(t, err)
	})

	t.Run("test builtin and container co-existing", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Builtin = &dfv1.Function{
			Name: "cat",
		}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no buildin function support in reduce vertices")
	})

	t.Run("test no image in container", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.Container.Image = ""
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "a customized image is required")
	})

	t.Run("test partitions", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[0].Partitions = pointer.Int32(2)
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `partitions should not > 1 for source vertices`)
		testObj.Spec.Vertices[0].Partitions = nil
		testObj.Spec.Vertices[1].Partitions = pointer.Int32(2)
		err = ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `partitions should not > 1 for non-keyed reduce vertices`)
	})

	t.Run("no storage", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.GroupBy.Storage = nil
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"storage" is missing`)
	})

	t.Run("no storage type", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.GroupBy.Storage = &dfv1.PBQStorage{}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `type of storage to use is missing`)
	})

	t.Run("both pvc and emptyDir", func(t *testing.T) {
		testObj := testReducePipeline.DeepCopy()
		testObj.Spec.Vertices[1].UDF.GroupBy.Storage = &dfv1.PBQStorage{
			PersistentVolumeClaim: &dfv1.PersistenceStrategy{},
			EmptyDir:              &corev1.EmptyDirVolumeSource{},
		}
		err := ValidatePipeline(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `either emptyDir or persistentVolumeClaim is allowed, not both`)
	})

}

func TestValidateVertex(t *testing.T) {
	t.Run("test invalid vertex name", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Name: "invalid.name",
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid vertex name")
	})

	goodContainers := []corev1.Container{{Name: "my-test-image", Image: "my-image:latest"}}
	badContainers := []corev1.Container{{Name: dfv1.CtrInit, Image: "my-image:latest"}}

	t.Run("bad min", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Name: "my-vertex",
			Scale: dfv1.Scale{
				Min: pointer.Int32(-1),
				Max: pointer.Int32(1),
			},
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not be smaller than 0")
	})

	t.Run("min > max", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Name: "my-vertex",
			Scale: dfv1.Scale{
				Min: pointer.Int32(2),
				Max: pointer.Int32(1),
			},
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "or equal to")
	})

	t.Run("good init container", func(t *testing.T) {
		v := dfv1.AbstractVertex{Name: "my-vertex", InitContainers: goodContainers}
		err := validateVertex(v)
		assert.NoError(t, err)
	})

	t.Run("bad init container name", func(t *testing.T) {
		v := dfv1.AbstractVertex{Name: "my-vertex", InitContainers: badContainers}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is reserved for containers created by numaflow")
	})

	t.Run("good sidecar container", func(t *testing.T) {
		v := dfv1.AbstractVertex{Name: "my-vertex", Sidecars: goodContainers}
		err := validateVertex(v)
		assert.NoError(t, err)
	})

	t.Run("bad sidecar container name", func(t *testing.T) {
		v := dfv1.AbstractVertex{Name: "my-vertex", Sidecars: badContainers}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is reserved for containers created by numaflow")
	})

	t.Run("sidecar on source vertex", func(t *testing.T) {
		v := dfv1.AbstractVertex{
			Name: "my-vertex",
			Source: &dfv1.Source{
				Generator: &dfv1.GeneratorSource{},
			},
			Sidecars: goodContainers,
		}
		err := validateVertex(v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"sidecars" are not supported for source vertices`)
	})
}

func TestValidateUDF(t *testing.T) {
	t.Run("bad window", func(t *testing.T) {
		udf := dfv1.UDF{
			GroupBy: &dfv1.GroupBy{
				Window: dfv1.Window{},
			},
		}
		err := validateUDF(udf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no windowing strategy specified")
	})

	t.Run("bad window length", func(t *testing.T) {
		udf := dfv1.UDF{
			GroupBy: &dfv1.GroupBy{
				Window: dfv1.Window{
					Fixed: &dfv1.FixedWindow{},
				},
			},
		}
		err := validateUDF(udf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"length" is missing`)
	})
}

func Test_validateSideInputs(t *testing.T) {
	testObj := testPipeline.DeepCopy()
	err := validateSideInputs(*testObj)
	assert.NoError(t, err)
	testObj.Spec.SideInputs = []dfv1.SideInput{
		{Name: ""},
	}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `name is missing`)

	testObj.Spec.SideInputs = []dfv1.SideInput{
		{Name: "s1"},
	}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `container is missing`)

	testObj.Spec.SideInputs[0].Container = &dfv1.Container{}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `image is missing`)

	testObj.Spec.SideInputs[0].Container.Image = "my-image:latest"
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `trigger is missing`)

	testObj.Spec.SideInputs[0].Trigger = &dfv1.SideInputTrigger{}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `schedule is required`)

	testObj.Spec.SideInputs[0].Trigger.Schedule = "@every 200s"
	testObj.Spec.SideInputs = append(testObj.Spec.SideInputs, dfv1.SideInput{
		Name: "s1",
		Container: &dfv1.Container{
			Image: "my-image:latest",
		},
		Trigger: &dfv1.SideInputTrigger{
			Schedule: "@every 200s",
		},
	})
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `is defined more than once`)

	testObj.Spec.SideInputs[1].Name = "s2"
	err = validateSideInputs(*testObj)
	assert.NoError(t, err)

	testObj.Spec.Vertices[1].SideInputs = []string{"s1", "s1"}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `is defined more than once`)

	testObj.Spec.Vertices[1].SideInputs = []string{"s1", "s3"}
	err = validateSideInputs(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `is not defined`)

	testObj.Spec.Vertices[1].SideInputs = []string{"s1", "s2"}
	err = validateSideInputs(*testObj)
	assert.NoError(t, err)
}

func Test_getCyclesFromVertex(t *testing.T) {
	tests := []struct {
		name                  string
		edges                 []dfv1.Edge
		startVertex           string
		expectedCycleVertices map[string]struct{}
	}{
		{
			name: "NoCycle",
			edges: []dfv1.Edge{
				{From: "A", To: "B"},
				{From: "B", To: "C"},
				{From: "B", To: "D"},
			},
			startVertex:           "A",
			expectedCycleVertices: map[string]struct{}{},
		},
		{
			name: "CycleToSelf",
			edges: []dfv1.Edge{
				{From: "A", To: "B"},
				{From: "B", To: "B"},
				{From: "B", To: "C"},
			},
			startVertex:           "A",
			expectedCycleVertices: map[string]struct{}{"B": {}},
		},
		{
			name: "CycleBackward",
			edges: []dfv1.Edge{
				{From: "A", To: "B"},
				{From: "B", To: "A"},
				{From: "B", To: "C"},
			},
			startVertex:           "A",
			expectedCycleVertices: map[string]struct{}{"A": {}},
		},
		{
			name: "Complicated",
			edges: []dfv1.Edge{
				{From: "A", To: "B"},
				{From: "B", To: "C"},
				{From: "B", To: "E"},
				{From: "A", To: "D"},
				{From: "D", To: "E"},
				{From: "E", To: "A"}, // this cycles
				{From: "E", To: "F"},
			},
			startVertex:           "A",
			expectedCycleVertices: map[string]struct{}{"A": {}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("running test: %q\n", tt.name)
			mappedEdges, err := toVerticesMappedByFrom(tt.edges, constructVerticesByName(tt.edges))
			assert.NoError(t, err)

			cyclesFound := mappedEdges.getCyclesFromVertex(&dfv1.AbstractVertex{Name: tt.startVertex}, make(map[string]struct{}))

			assert.Equal(t, len(tt.expectedCycleVertices), len(cyclesFound))
			for cycleFound := range cyclesFound {
				assert.Contains(t, tt.expectedCycleVertices, cycleFound)
			}
		})
	}

}

func constructVerticesByName(edges []dfv1.Edge) map[string]*dfv1.AbstractVertex {
	mappedVertices := make(map[string]*dfv1.AbstractVertex)
	for _, edge := range edges {
		mappedVertices[edge.From] = &dfv1.AbstractVertex{Name: edge.From} // fine if we see the same one twice and overwrite
		mappedVertices[edge.To] = &dfv1.AbstractVertex{Name: edge.To}
	}
	return mappedVertices
}

func Test_validateCycles(t *testing.T) {
	tests := []struct {
		name         string
		pipelineSpec *dfv1.PipelineSpec
		success      bool
	}{
		{
			name: "NoCycle",
			pipelineSpec: &dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{Name: "A", Source: &dfv1.Source{}},
					{Name: "B", UDF: &dfv1.UDF{}},
					{Name: "C", UDF: &dfv1.UDF{}},
					{Name: "D", UDF: &dfv1.UDF{}},
					{Name: "E", UDF: &dfv1.UDF{}},
					{Name: "F", Source: &dfv1.Source{}},
					{Name: "G", UDF: &dfv1.UDF{}},
				},
				Edges: []dfv1.Edge{
					{From: "A", To: "B"},
					{From: "B", To: "C"},
					{From: "A", To: "D"},
					{From: "D", To: "E"},
					{From: "E", To: "B"},
					{From: "F", To: "G"},
					{From: "G", To: "D"},
				},
			},
			success: true,
		},
		{
			name: "CycleToSelf-NoReduce",
			pipelineSpec: &dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{Name: "A", Source: &dfv1.Source{}},
					{Name: "B", UDF: &dfv1.UDF{GroupBy: &dfv1.GroupBy{}}}, //Reduce vertex
					{Name: "C", UDF: &dfv1.UDF{}},
					{Name: "D", UDF: &dfv1.UDF{}},
					{Name: "E", UDF: &dfv1.UDF{}},
					{Name: "F", Source: &dfv1.Source{}},
					{Name: "G", UDF: &dfv1.UDF{}},
				},
				Edges: []dfv1.Edge{
					{From: "A", To: "B"},
					{From: "B", To: "C"},
					{From: "C", To: "C"}, // cycle to self
					{From: "C", To: "E"},
					{From: "A", To: "D"},
					{From: "D", To: "E"},
					{From: "F", To: "G"},
					{From: "G", To: "D"},
				},
			},
			success: true,
		},
		{
			name: "CycleToSelf-CycleIsReduce",
			pipelineSpec: &dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{Name: "A", Source: &dfv1.Source{}},
					{Name: "B", UDF: &dfv1.UDF{GroupBy: &dfv1.GroupBy{}}}, //Reduce vertex
					{Name: "C", UDF: &dfv1.UDF{}},
					{Name: "D", UDF: &dfv1.UDF{}},
					{Name: "E", UDF: &dfv1.UDF{}},
					{Name: "F", Source: &dfv1.Source{}},
					{Name: "G", UDF: &dfv1.UDF{}},
				},
				Edges: []dfv1.Edge{
					{From: "A", To: "B"},
					{From: "B", To: "B"}, // cycle to self
					{From: "B", To: "C"},
					{From: "A", To: "D"},
					{From: "D", To: "E"},
					{From: "F", To: "G"},
					{From: "G", To: "D"},
				},
			},
			success: false,
		},
		{
			name: "CycleToSelf-ReduceAhead",
			pipelineSpec: &dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{Name: "A", Source: &dfv1.Source{}},
					{Name: "B", UDF: &dfv1.UDF{}},
					{Name: "C", UDF: &dfv1.UDF{GroupBy: &dfv1.GroupBy{}}}, //Reduce vertex
					{Name: "D", UDF: &dfv1.UDF{}},
					{Name: "E", UDF: &dfv1.UDF{}},
					{Name: "F", Source: &dfv1.Source{}},
					{Name: "G", UDF: &dfv1.UDF{}},
				},
				Edges: []dfv1.Edge{
					{From: "A", To: "B"},
					{From: "B", To: "B"}, // cycle to self
					{From: "B", To: "C"},
					{From: "A", To: "D"},
					{From: "D", To: "E"},
					{From: "F", To: "G"},
					{From: "G", To: "D"},
				},
			},
			success: false,
		},
		{
			name: "CycleBackward-ReduceAhead",
			pipelineSpec: &dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{Name: "A", Source: &dfv1.Source{}},
					{Name: "B", UDF: &dfv1.UDF{}}, //Reduce vertex
					{Name: "C", UDF: &dfv1.UDF{}},
					{Name: "D", UDF: &dfv1.UDF{}},
					{Name: "E", UDF: &dfv1.UDF{GroupBy: &dfv1.GroupBy{}}}, //Reduce vertex
					{Name: "F", Source: &dfv1.Source{}},
					{Name: "G", UDF: &dfv1.UDF{}},
					{Name: "H", UDF: &dfv1.UDF{}},
				},
				Edges: []dfv1.Edge{
					{From: "A", To: "B"},
					{From: "B", To: "C"},
					{From: "A", To: "D"},
					{From: "D", To: "E"},
					{From: "F", To: "G"},
					{From: "G", To: "D"},
					{From: "D", To: "G"}, // cycle backward
					{From: "E", To: "H"},
				},
			},
			success: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("running test: %q\n", tt.name)
			err := validateCycles(tt.pipelineSpec)
			if tt.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_validateIdleSource(t *testing.T) {
	testObj := testPipeline.DeepCopy()
	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold:   &metav1.Duration{Duration: 5 * time.Second},
		IncrementBy: &metav1.Duration{Duration: 5 * time.Second},
	}
	err := validateIdleSource(*testObj)
	assert.NoError(t, err)

	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold: nil,
	}
	err = validateIdleSource(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid idle source watermark config, threshold is missing`)

	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold: &metav1.Duration{Duration: 0 * time.Second},
	}
	err = validateIdleSource(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid idle source watermark config, threshold should be greater than 0`)

	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold:   &metav1.Duration{Duration: 5 * time.Second},
		IncrementBy: nil,
	}
	err = validateIdleSource(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid idle source watermark config, incrementBy is missing`)

	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold:   &metav1.Duration{Duration: 5 * time.Second},
		IncrementBy: &metav1.Duration{Duration: 0 * time.Second},
	}
	err = validateIdleSource(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid idle source watermark config, incrementBy should be greater than 0`)

	testObj.Spec.Watermark.IdleSource = &dfv1.IdleSource{
		Threshold:   &metav1.Duration{Duration: 2 * time.Second},
		IncrementBy: &metav1.Duration{Duration: 5 * time.Second},
	}
	err = validateIdleSource(*testObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid idle source watermark config, threshold should be greater than or equal to incrementBy`)
}
