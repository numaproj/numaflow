package v1alpha1

// VertexInstance is a wrapper of a vertex instance, which contains the vertex spec and the instance information such as hostname and replica index.
type VertexInstance struct {
	Vertex   *Vertex `json:"vertex,omitempty" protobuf:"bytes,1,opt,name=vertex"`
	Hostname string  `json:"hostname,omitempty" protobuf:"bytes,2,opt,name=hostname"`
	Replica  int32   `json:"replica,omitempty" protobuf:"varint,3,opt,name=replica"`
}
