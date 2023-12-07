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
	udTransformers := make(map[string]dfv1.AbstractVertex)
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
			if len(pl.GetToEdges(v.Name)) == 0 || len(pl.GetFromEdges(v.Name)) > 0 {
				return fmt.Errorf("invalid vertex %q, source must have 0 from edges and at least 1 to edge", v.Name)
			}
			sources[v.Name] = v
			if v.Source.UDTransformer != nil {
				udTransformers[v.Name] = v
			}
		}
		if v.Sink != nil {
			if v.Source != nil || v.UDF != nil {
				return fmt.Errorf("invalid vertex %q, only one of 'source', 'sink' and 'udf' can be specified", v.Name)
			}
			if len(pl.GetFromEdges(v.Name)) == 0 || len(pl.GetToEdges(v.Name)) > 0 {
				return fmt.Errorf("invalid vertex %q, sink must have 0 to edges and at least 1 from edge", v.Name)
			}
			sinks[v.Name] = v
		}
		if v.UDF != nil {
			if v.Source != nil || v.Sink != nil {
				return fmt.Errorf("invalid vertex %q, only one of 'source', 'sink' and 'udf' can be specified", v.Name)
			}
			if len(pl.GetToEdges(v.Name)) == 0 || len(pl.GetFromEdges(v.Name)) == 0 {
				return fmt.Errorf("invalid vertex %q, UDF must have to and from edges", v.Name)
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
		if s.IsUDSource() {
			if s.Source.UDSource.Container == nil || s.Source.UDSource.Container.Image == "" {
				return fmt.Errorf("invalid user-defined source vertex %q, a customized image is required", k)
			}
			if s.Source.HTTP != nil || s.Source.Kafka != nil || s.Source.Nats != nil || s.Source.Generator != nil {
				return fmt.Errorf("invalid user-defined source vertex %q, only one of 'http', 'kafka', 'nats', 'generator' and 'udSource' can be specified", k)
			}
		}
	}

	for k, t := range udTransformers {
		transformer := t.Source.UDTransformer
		if transformer.Container != nil {
			if transformer.Container.Image == "" && transformer.Builtin == nil {
				return fmt.Errorf("invalid source vertex %q, either specify a builtin transformer, or a customized image", k)
			}
			if transformer.Container.Image != "" && transformer.Builtin != nil {
				return fmt.Errorf("invalid source vertex %q, can not specify both builtin transformer, and a customized image", k)
			}
		} else if transformer.Builtin == nil {
			return fmt.Errorf("invalid source vertex %q, either specify a builtin transformer, or a customized image", k)
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
	toFromEdge := make(map[string]bool)
	for _, e := range pl.Spec.Edges {
		if e.From == "" || e.To == "" {
			return fmt.Errorf("invalid edge: both from and to need to be specified")
		}
		if !names[e.From] {
			return fmt.Errorf("invalid edge: no vertex named %q", e.From)
		}
		if !names[e.To] {
			return fmt.Errorf("invalid edge: no vertex named %q", e.To)
		}
		namesInEdges[e.From] = true
		namesInEdges[e.To] = true
		// check for redundant edges
		if _, existing := toFromEdge[e.From+e.To]; existing {
			return fmt.Errorf("cannot define multiple edges from vertex %q to vertex %q", e.From, e.To)
		} else {
			toFromEdge[e.From+e.To] = true
		}
	}

	if len(namesInEdges) != len(names) {
		return fmt.Errorf("not all the vertex names are defined in edges")
	}

	if isAForest(pl) {
		return fmt.Errorf("invalid pipeline, cannot be disjointed")
	}

	// Prevent pipelines with Cycles in the case that there is a Reduce Vertex at the point of the cycle or to the right of it.
	// Whenever there's a cycle, there will inherently be "late data", and we don't want late data for a Reduce Vertex, which may
	// have already "closed the book" on the data's time window.
	if err := validateCycles(&pl.Spec); err != nil {
		return err
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

	if err := validateSideInputs(*pl); err != nil {
		return err
	}

	if err := validateIdleSource(*pl); err != nil {
		return err
	}

	return nil
}

// validateIdleSource validates the idle source watermark config.
// The threshold should be greater than or equal to incrementBy.
func validateIdleSource(pl dfv1.Pipeline) error {
	if pl.Spec.Watermark.IdleSource != nil {
		if pl.Spec.Watermark.IdleSource.Threshold == nil {
			return fmt.Errorf("invalid idle source watermark config, threshold is missing")
		} else if pl.Spec.Watermark.IdleSource.Threshold.Duration <= 0 {
			return fmt.Errorf("invalid idle source watermark config, threshold should be greater than 0")
		} else if pl.Spec.Watermark.IdleSource.IncrementBy == nil {
			return fmt.Errorf("invalid idle source watermark config, incrementBy is missing")
		} else if pl.Spec.Watermark.IdleSource.IncrementBy.Duration <= 0 {
			return fmt.Errorf("invalid idle source watermark config, incrementBy should be greater than 0")
		} else if pl.Spec.Watermark.IdleSource.Threshold.Duration < pl.Spec.Watermark.IdleSource.IncrementBy.Duration {
			return fmt.Errorf("invalid idle source watermark config, threshold should be greater than or equal to incrementBy")
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
	if v.Partitions != nil {
		if *v.Partitions < 0 {
			return fmt.Errorf("vertex %q: number of partitions should not be smaller than 0", v.Name)
		}
		if *v.Partitions > 1 && v.IsReduceUDF() && !v.UDF.GroupBy.Keyed {
			return fmt.Errorf("vertex %q: partitions should not > 1 for non-keyed reduce vertices", v.Name)
		}
		if *v.Partitions > 1 && v.IsASource() {
			return fmt.Errorf("vertex %q: partitions should not > 1 for source vertices", v.Name)
		}
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

func validateSideInputs(pl dfv1.Pipeline) error {
	sideInputs := make(map[string]bool)
	for _, si := range pl.Spec.SideInputs {
		if si.Name == "" {
			return fmt.Errorf("side input name is missing")
		}
		if _, existing := sideInputs[si.Name]; existing {
			return fmt.Errorf("side input %q is defined more than once", si.Name)
		}
		sideInputs[si.Name] = true
		if si.Container == nil {
			return fmt.Errorf("side input %q: container is missing", si.Name)
		}
		if si.Container.Image == "" {
			return fmt.Errorf("side input %q: image is missing", si.Name)
		}
		if si.Trigger == nil {
			return fmt.Errorf("side input %q: trigger is missing", si.Name)
		}
		if len(si.Trigger.Schedule) == 0 {
			return fmt.Errorf("side input %q: schedule is required", si.Name)
		}
	}
	for _, v := range pl.Spec.Vertices {
		namesInVertex := make(map[string]bool)
		for _, si := range v.SideInputs {
			if _, existing := sideInputs[si]; !existing {
				return fmt.Errorf("vertex %q: side input %q is not defined", v.Name, si)
			}
			if _, existing := namesInVertex[si]; existing {
				return fmt.Errorf("vertex %q: side input %q is defined more than once", v.Name, si)
			}
			namesInVertex[si] = true
		}
	}
	return nil
}

func isReservedContainerName(name string) bool {
	return name == dfv1.CtrInit ||
		name == dfv1.CtrMain ||
		name == dfv1.CtrUdf ||
		name == dfv1.CtrUdsink ||
		name == dfv1.CtrUdtransformer ||
		name == dfv1.CtrUdsource ||
		name == dfv1.CtrUdSideInput ||
		name == dfv1.CtrInitSideInputs ||
		name == dfv1.CtrSideInputsWatcher
}

// validateCycles verifies that there are no invalid cycles in the pipeline.
// An invalid cycle has a Reduce Vertex at or to the right of the cycle. Whenever there's a cycle,
// there will inherently be "late data", and we don't want late data for a Reduce Vertex, which may
// have already "closed the book" on the data's time window.
func validateCycles(pipelineSpec *dfv1.PipelineSpec) error {
	verticesByName := pipelineSpec.GetVerticesByName()
	edges, err := toVerticesMappedByFrom(pipelineSpec.Edges, verticesByName)
	if err != nil {
		return err
	}

	// first find the cycles, if any
	cycles, err := getCycles(pipelineSpec)
	if err != nil {
		return err
	}
	// need to make sure none of the cycles have a Reduce Vertex at or to the right of the cycle
	for cycleVertexName := range cycles {
		cycleVertex, found := verticesByName[cycleVertexName]
		if !found {
			return fmt.Errorf("something went wrong: no Vertex found with name %q", cycleVertexName)
		}
		invalidReduce := edges.findVertex(cycleVertex, map[string]struct{}{}, func(v *dfv1.AbstractVertex) bool {
			return v.IsReduceUDF()
		})
		if invalidReduce {
			return fmt.Errorf("there's a Reduce Vertex at or to the right of a Cycle occurring at Vertex %q", cycleVertexName)
		}
	}

	return nil
}

// getCycles locates the vertices where there's a Cycle, if any
// eg. if A->B->A, then return A
// Since there are multiple Sources, and since each Source produces a Tree, then we can return multiple Cycles
func getCycles(pipelineSpec *dfv1.PipelineSpec) (map[string]struct{}, error) {
	edges, err := toVerticesMappedByFrom(pipelineSpec.Edges, pipelineSpec.GetVerticesByName())
	if err != nil {
		return nil, err
	}

	sources := pipelineSpec.GetSourcesByName()
	cycles := map[string]struct{}{} // essentially a Set of cycle Vertex names

	// consolidate the Cycles from all Sources
	for _, sourceVertex := range sources {
		cyclesFromSource := edges.getCyclesFromVertex(sourceVertex, map[string]struct{}{})
		for cycleVertex := range cyclesFromSource {
			cycles[cycleVertex] = struct{}{}
		}
	}

	return cycles, nil
}

// getCyclesFromVertex returns the cycles detected if any, starting from startVertex
// This is a recursive function. Each iteration we keep track of the visited Vertices in order to detect a cycle.
func (edges verticesByFrom) getCyclesFromVertex(startVertex *dfv1.AbstractVertex, visited map[string]struct{}) map[string]struct{} {

	toVertices, found := edges[startVertex.Name]
	// base case: no Edges stem from this Vertex
	if !found {
		return map[string]struct{}{}
	}

	// check for cycle
	_, alreadyVisited := visited[startVertex.Name]
	if alreadyVisited {
		return map[string]struct{}{startVertex.Name: {}}
	}
	// add this Vertex to our Set
	visited[startVertex.Name] = struct{}{}

	// recurse the Edges of this Vertex, looking for cycles
	cyclesFound := make(map[string]struct{})
	for _, toVertex := range toVertices {
		newCycles := edges.getCyclesFromVertex(toVertex, visited)
		for cycleVertex := range newCycles {
			cyclesFound[cycleVertex] = struct{}{}
		}
	}

	delete(visited, startVertex.Name) // pop

	return cyclesFound
}

// findVertex determines if any Vertex starting from this one meets some condition
// This is a recursive function. Each iteration we keep track of the visited Vertices in order not to get in an infinite loop
func (edges verticesByFrom) findVertex(startVertex *dfv1.AbstractVertex, visited map[string]struct{}, f func(*dfv1.AbstractVertex) bool) bool {

	// first try the condition on this vertex
	if f(startVertex) {
		return true
	}

	toVertices, found := edges[startVertex.Name]
	// base case: no Edges stem from this Vertex
	if !found {
		return false
	}

	// if we've arrived at a cycle, then stop
	_, alreadyVisited := visited[startVertex.Name]
	if alreadyVisited {
		return false
	}
	// keep track of visited vertices so we don't get into an infinite loop
	visited[startVertex.Name] = struct{}{}

	// recurse
	for _, toVertex := range toVertices {
		if edges.findVertex(toVertex, visited, f) {
			return true
		}
	}

	delete(visited, startVertex.Name) // pop

	return false
}

type verticesByFrom map[string][]*dfv1.AbstractVertex

// toVerticesMappedByFrom is a helper function to create a map of "To Vertices" from their "From Vertex"
func toVerticesMappedByFrom(edges []dfv1.Edge, verticesByName map[string]*dfv1.AbstractVertex) (verticesByFrom, error) {
	mappedEdges := make(verticesByFrom)
	for _, edge := range edges {
		_, found := mappedEdges[edge.From]
		if !found {
			mappedEdges[edge.From] = make([]*dfv1.AbstractVertex, 0)
		}
		toVertex, found := verticesByName[edge.To]
		if !found {
			return nil, fmt.Errorf("no vertex found of name %q", edge.To)
		}
		mappedEdges[edge.From] = append(mappedEdges[edge.From], toVertex)
	}
	return mappedEdges, nil
}

// isAForest determines if the pipeline is a disjointed graph ie. multiple pipelines defined in the spec
func isAForest(pl *dfv1.Pipeline) bool {

	visited := map[string]struct{}{}
	buildVisitedMap(pl.Spec.Vertices[0].Name, visited, pl)

	// if we have not visited every vertex in the graph, it is a forest
	return len(visited) != len(pl.Spec.Vertices)

}

// buildVisitedMap is a helper function that traverses the pipeline using DFS
// This is a recursive function. Each iteration we are building our visited map to check in the parent function.
func buildVisitedMap(vtxName string, visited map[string]struct{}, pl *dfv1.Pipeline) {

	visited[vtxName] = struct{}{}

	// construct list of all to and from vertices
	neighbors := make(map[string]string)
	toEdges := pl.GetToEdges(vtxName)
	fromEdges := pl.GetFromEdges(vtxName)
	for _, e := range toEdges {
		neighbors[e.To] = e.To
	}
	for _, e := range fromEdges {
		neighbors[e.From] = e.From
	}

	// visit all to and from vertices
	for _, v := range neighbors {
		if _, alreadyVisited := visited[v]; !alreadyVisited {
			buildVisitedMap(v, visited, pl)
		}
	}

}
