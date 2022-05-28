package types

import dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

type SourceMetadata struct {
	Vertex   *dfv1.Vertex
	Hostname string
	Replica  int
}
