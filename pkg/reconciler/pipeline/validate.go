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
	"fmt"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func ValidatePipeline(pl *dfv1.Pipeline) error {
	if pl == nil {
		return fmt.Errorf("nil pipeline")
	}

	if errs := k8svalidation.IsDNS1035Label(pl.Name); len(errs) > 0 {
		return fmt.Errorf("invalid pipeline name %q, %v", pl.Name, errs)
	}

	if len(pl.Spec.Vertices) == 0 {
		return fmt.Errorf("empty vertices")
	}
	if len(pl.Spec.Edges) == 0 {
		return fmt.Errorf("no edges defined")
	}
	names := make(map[string]bool)
	sources := make(map[string]dfv1.AbstractVertex)
	sinks := make(map[string]dfv1.AbstractVertex)
	mapUdfs := make(map[string]dfv1.AbstractVertex)
	reduceUdfs := make(map[string]dfv1.AbstractVertex)
	for _, v := range pl.Spec.Vertices {
		if names[v.Name] {
			return fmt.Errorf("duplicate vertex name %q", v.Name)
		}
		names[v.Name] = true

		if v.Source == nil && v.Sink == nil && v.UDF == nil {
			return fmt.Errorf("invalid vertex %q, it could only be either a source, or a sink, or a UDF", v.Name)
		}
		if v.Source != nil {
			if v.Sink != nil || v.UDF != nil {
				return fmt.Errorf("invalid vertex %q, only one of 'source', 'sink' and 'udf' can be specified", v.Name)
			}
			sources[v.Name] = v
		}
		if v.Sink != nil {
			if v.Source != nil || v.UDF != nil {
				return fmt.Errorf("invalid vertex %q, only one of 'source', 'sink' and 'udf' can be specified", v.Name)
			}
			sinks[v.Name] = v
		}
		if v.UDF != nil {
			if v.Source != nil || v.Sink != nil {
				return fmt.Errorf("invalid vertex %q, only one of 'source', 'sink' and 'udf' can be specified", v.Name)
			}
			if v.UDF.GroupBy != nil {
				reduceUdfs[v.Name] = v
			} else {
				mapUdfs[v.Name] = v
			}
		}
	}

	if len(sources) == 0 {
		return fmt.Errorf("pipeline has no source, at least one vertex with 'source' defined is required")
	}

	if len(sinks) == 0 {
		return fmt.Errorf("pipeline has no sink, at least one vertex with 'sink' defined is required")
	}

	for k, s := range sources {
		if s.Source.UdTransformer != nil && s.Source.UdTransformer.Container != nil && s.Source.UdTransformer.Container.Image == "" {
			return fmt.Errorf("invalid vertex %q, can not specify an empty image for a customized source data transformer", k)
		}
	}

	for k, u := range mapUdfs {
		if u.UDF.Container != nil {
			if u.UDF.Container.Image == "" && u.UDF.Builtin == nil {
				return fmt.Errorf("invalid vertex %q, either specify a builtin function, or a customized image", k)
			}
			if u.UDF.Container.Image != "" && u.UDF.Builtin != nil {
				return fmt.Errorf("invalid vertex %q, can not specify both builtin function, and a customized image", k)
			}
		} else if u.UDF.Builtin == nil {
			return fmt.Errorf("invalid vertex %q, either specify a builtin function, or a customized image", k)
		}
	}

	for k, u := range reduceUdfs {
		if u.UDF.Builtin != nil {
			// No builtin function supported for reduce vertices.
			return fmt.Errorf("invalid vertex %q, there's no buildin function support in reduce vertices", k)
		}
		if u.UDF.Container != nil {
			if u.UDF.Container.Image == "" {
				return fmt.Errorf("invalid vertex %q, a customized image is required", k)
			}
		}
	}

	namesInEdges := make(map[string]bool)
	for _, e := range pl.Spec.Edges {
		if e.From == "" || e.To == "" {
			return fmt.Errorf("invalid edge: both from and to need to be specified")
		}
		if e.From == e.To {
			return fmt.Errorf("invalid edge: same from and to")
		}
		if !names[e.From] {
			return fmt.Errorf("invalid edge: no vertex named %q", e.From)
		}
		if !names[e.To] {
			return fmt.Errorf("invalid edge: no vertex named %q", e.To)
		}
		if _, existing := sources[e.To]; existing {
			return fmt.Errorf("source vertex %q can not be define as 'to'", e.To)
		}
		if _, existing := sinks[e.From]; existing {
			return fmt.Errorf("sink vertex %q can not be define as 'from'", e.To)
		}
		if e.Parallelism != nil {
			if _, ok := reduceUdfs[e.To]; !ok {
				return fmt.Errorf(`invalid edge (%s - %s), "parallelism" is not allowed for an edge leading to a non-reduce vertex`, e.From, e.To)
			}
			if *e.Parallelism < 1 {
				return fmt.Errorf(`invalid edge (%s - %s), "parallelism" is < 1`, e.From, e.To)
			}
			if *e.Parallelism > 1 && !reduceUdfs[e.To].UDF.GroupBy.Keyed {
				// We only support single partition non-keyed windowing.
				return fmt.Errorf(`invalid edge (%s - %s), "parallelism" should not > 1 for non-keyed windowing`, e.From, e.To)
			}
			if _, ok := sources[e.From]; ok && reduceUdfs[e.To].UDF.GroupBy.Keyed {
				// Source vertex can not lead to a keyed reduce vertex, because the keys coming from sources are undeterminable.
				return fmt.Errorf(`invalid spec (%s - %s), "keyed" should not be true for a reduce vertex which has data coming from a source vertex`, e.From, e.To)
			}
		}
		namesInEdges[e.From] = true
		namesInEdges[e.To] = true
	}
	if len(namesInEdges) != len(names) {
		return fmt.Errorf("not all the vertex names are defined in edges")
	}

	// Do not support N FROM -> 1 TO for now.
	toInEdges := make(map[string]bool)
	for _, e := range pl.Spec.Edges {
		if _, existing := toInEdges[e.To]; existing {
			return fmt.Errorf("vertex %q has multiple 'from', which is not supported yet", e.To)
		}
		toInEdges[e.To] = true
	}

	for _, v := range pl.Spec.Vertices {
		if err := validateVertex(v); err != nil {
			return err
		}
		// The length of "{pipeline}-{vertex}-headless" can not be longer than 63.
		if errs := k8svalidation.IsDNS1035Label(fmt.Sprintf("%s-%s-headless", pl.Name, v.Name)); len(errs) > 0 {
			return fmt.Errorf("the length of the pipeline name plus the vertex name is over the max limit. (%s-%s), %v", pl.Name, v.Name, errs)
		}
	}
	return nil
}

func validateVertex(v dfv1.AbstractVertex) error {
	if errs := k8svalidation.IsDNS1035Label(v.Name); len(errs) > 0 {
		return fmt.Errorf("invalid vertex name %q, %v", v.Name, errs)
	}
	min, max := int32(0), int32(dfv1.DefaultMaxReplicas)
	if v.Scale.Min != nil {
		min = *v.Scale.Min
	}
	if v.Scale.Max != nil {
		max = *v.Scale.Max
	}
	if min < 0 {
		return fmt.Errorf("vertex %q: min number of replicas should not be smaller than 0", v.Name)
	}
	if min > max {
		return fmt.Errorf("vertex %q: max number of replicas should be greater than or equal to min", v.Name)
	}
	for _, ic := range v.InitContainers {
		if isReservedContainerName(ic.Name) {
			return fmt.Errorf("vertex %q: init container name %q is reserved for containers created by numaflow", v.Name, ic.Name)
		}
	}
	if len(v.Sidecars) != 0 && v.Source != nil {
		return fmt.Errorf(`vertex %q: "sidecars" are not supported for source vertices`, v.Name)
	}
	for _, sc := range v.Sidecars {
		if isReservedContainerName(sc.Name) {
			return fmt.Errorf("vertex %q: sidecar container name %q is reserved for containers created by numaflow", v.Name, sc.Name)
		}
	}
	if v.UDF != nil {
		return validateUDF(*v.UDF)
	}
	return nil
}

func validateUDF(udf dfv1.UDF) error {
	if udf.GroupBy != nil {
		f := udf.GroupBy.Window.Fixed
		s := udf.GroupBy.Window.Sliding
		storage := udf.GroupBy.Storage
		if f == nil && s == nil {
			return fmt.Errorf(`invalid "groupBy.window", no windowing strategy specified`)
		}

		if f != nil && s != nil {
			return fmt.Errorf(`invalid "groupBy.window", either fixed or sliding is allowed, not both`)
		}

		if f != nil && f.Length == nil {
			return fmt.Errorf(`invalid "groupBy.window.fixed", "length" is missing`)
		}

		if s != nil && (s.Length == nil) {
			return fmt.Errorf(`invalid "groupBy.window.sliding", "length" is missing`)
		}
		if s != nil && (s.Slide == nil) {
			return fmt.Errorf(`invalid "groupBy.window.sliding", "slide" is missing`)
		}
		if storage == nil {
			return fmt.Errorf(`invalid "groupBy", "storage" is missing`)
		}
		if storage.PersistentVolumeClaim == nil && storage.EmptyDir == nil {
			return fmt.Errorf(`invalid "groupBy.storage", type of storage to use is missing`)
		}
		if storage.PersistentVolumeClaim != nil && storage.EmptyDir != nil {
			return fmt.Errorf(`invalid "groupBy.storage", either emptyDir or persistentVolumeClaim is allowed, not both`)
		}
	}
	return nil
}

func isReservedContainerName(name string) bool {
	return name == dfv1.CtrInit ||
		name == dfv1.CtrMain ||
		name == dfv1.CtrUdf ||
		name == dfv1.CtrUdsink ||
		name == dfv1.CtrUdtransformer
}
