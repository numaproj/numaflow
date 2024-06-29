package isb

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/apis/proto/isb"
)

// MarshalBinary encodes Message to proto bytes.
func (m Message) MarshalBinary() ([]byte, error) {
	pb := &isb.Message{
		Header: &isb.Header{
			MessageInfo: &isb.MessageInfo{
				EventTime: timestamppb.New(m.Header.MessageInfo.EventTime),
				IsLate:    m.Header.MessageInfo.IsLate,
			},
			Kind: isb.MessageKind(m.Header.Kind),
			Id: &isb.MessageID{
				VertexName: m.Header.ID.VertexName,
				Offset:     m.Header.ID.Offset,
				Index:      m.Header.ID.Index,
			},
			Keys:    m.Header.Keys,
			Headers: m.Header.Headers,
		},
		Body: &isb.Body{
			Payload: m.Body.Payload,
		},
	}
	return proto.Marshal(pb)
}

// UnmarshalBinary decodes Message from the proto bytes.
// Even though the header and body are pointers in the proto, we don't need
// nil checks because while marshalling, we always set the default values.
func (m *Message) UnmarshalBinary(data []byte) error {
	pb := &isb.Message{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	// no nil checks are performed because Marshalling and Unmarshalling
	// are completely controlled by numaflow. It is best not to do nil checks
	// and get panics so we can fix the root cause.
	
	m.Header = Header{
		MessageInfo: MessageInfo{
			EventTime: pb.Header.MessageInfo.EventTime.AsTime(),
			IsLate:    pb.Header.MessageInfo.IsLate,
		},
		Kind: MessageKind(pb.Header.Kind),
		ID: MessageID{
			VertexName: pb.Header.Id.VertexName,
			Offset:     pb.Header.Id.Offset,
			Index:      pb.Header.Id.Index,
		},
		Keys:    pb.Header.Keys,
		Headers: pb.Header.Headers,
	}

	m.Body.Payload = pb.Body.Payload

	return nil
}

// MarshalBinary encodes Header to proto bytes.
func (h Header) MarshalBinary() ([]byte, error) {
	pb := &isb.Header{
		MessageInfo: &isb.MessageInfo{
			EventTime: timestamppb.New(h.MessageInfo.EventTime),
			IsLate:    h.MessageInfo.IsLate,
		},
		Kind:    isb.MessageKind(h.Kind),
		Id:      &isb.MessageID{VertexName: h.ID.VertexName, Offset: h.ID.Offset, Index: h.ID.Index},
		Keys:    h.Keys,
		Headers: h.Headers,
	}
	return proto.Marshal(pb)
}

// UnmarshalBinary decodes Header from the proto bytes.
func (h *Header) UnmarshalBinary(data []byte) error {
	pb := &isb.Header{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	// no nil checks are performed because Marshalling and Unmarshalling
	// are completely controlled by numaflow. It is best not to do nil checks
	// and get panics so we can fix the root cause.
	
	h.MessageInfo = MessageInfo{
		EventTime: pb.MessageInfo.EventTime.AsTime(),
		IsLate:    pb.MessageInfo.IsLate,
	}

	h.Kind = MessageKind(pb.Kind)

	h.ID = MessageID{
		VertexName: pb.Id.VertexName,
		Offset:     pb.Id.Offset,
		Index:      pb.Id.Index,
	}

	h.Keys = pb.Keys
	h.Headers = pb.Headers

	return nil
}

// MarshalBinary encodes MessageID to proto bytes.
func (id MessageID) MarshalBinary() ([]byte, error) {
	pb := &isb.MessageID{
		VertexName: id.VertexName,
		Offset:     id.Offset,
		Index:      id.Index,
	}
	return proto.Marshal(pb)
}

// UnmarshalBinary decodes MessageID from proto bytes.
func (id *MessageID) UnmarshalBinary(data []byte) error {
	pb := &isb.MessageID{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	id.VertexName = pb.VertexName
	id.Offset = pb.Offset
	id.Index = pb.Index

	return nil
}
