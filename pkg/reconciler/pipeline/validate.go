package pipeline

import (
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func ValidatePipeline(pl *dfv1.Pipeline) error {
	if pl == nil {
		return fmt.Errorf("nil pipeline")
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
	udfs := make(map[string]dfv1.AbstractVertex)
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
			udfs[v.Name] = v
		}
	}

	if len(sources) == 0 {
		return fmt.Errorf("pipeline has no source, at lease one vertex with 'source' defined is required")
	}

	if len(sinks) == 0 {
		return fmt.Errorf("pipeline has no sink, at lease one vertex with 'sink' defined is required")
	}

	for k, u := range udfs {
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
		if e.Conditions != nil && len(e.Conditions.KeyIn) > 0 {
			if _, ok := sources[e.From]; ok { // Source vertex should not do conditional forwarding
				return fmt.Errorf("invalid edge, \"conditions.keysIn\" not allowed for %q", e.From)
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
	}
	return nil
}

func validateVertex(v dfv1.AbstractVertex) error {
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
		if ic.Name == dfv1.CtrInit ||
			ic.Name == dfv1.CtrMain ||
			ic.Name == dfv1.CtrUdf ||
			ic.Name == dfv1.CtrUdsink {
			return fmt.Errorf("vertex %q: init container name %q is reserved for containers created by numaflow", v.Name, ic.Name)
		}
	}
	if v.UDF != nil {
		return validateUDF(*v.UDF)
	}
	return nil
}

func validateUDF(udf dfv1.UDF) error {
	if udf.GroupBy != nil {
		if x := udf.GroupBy.Window.Fixed; x == nil {
			return fmt.Errorf(`invalid "groupBy.window", no windowing strategy specified`)
		} else {
			if x.Length == nil {
				return fmt.Errorf(`invalid "groupBy.window", "length" is missing`)
			}
		}
	}
	return nil
}
