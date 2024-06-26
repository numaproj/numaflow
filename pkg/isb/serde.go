package isb

import (
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/apis/proto/isb"
)

// Marshal encodes Message to the protobuf format
func (m Message) Marshal() ([]byte, error) {
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

// Unmarshal decodes Message from the protobuf format
func (m *Message) Unmarshal(data []byte) error {
	pb := &isb.Message{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	if pb.Header != nil {
		m.Header = Header{}

		if pb.Header.MessageInfo != nil {
			m.Header.MessageInfo = MessageInfo{
				EventTime: pb.Header.MessageInfo.EventTime.AsTime(),
				IsLate:    pb.Header.MessageInfo.IsLate,
			}
		}

		m.Header.Kind = MessageKind(pb.Header.Kind)

		if pb.Header.Id != nil {
			m.Header.ID = MessageID{
				VertexName: pb.Header.Id.VertexName,
				Offset:     pb.Header.Id.Offset,
				Index:      pb.Header.Id.Index,
			}
		}

		m.Header.Keys = pb.Header.Keys
		m.Header.Headers = pb.Header.Headers
		m.Body.Payload = pb.Body.Payload
	}

	return nil
}

// Marshal encodes Header to the protobuf binary format
func (h Header) Marshal() ([]byte, error) {
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

// Unmarshal decodes Header from the protobuf binary format
func (h *Header) Unmarshal(data []byte) error {
	pb := &isb.Header{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}

	if pb.MessageInfo != nil {
		h.MessageInfo = MessageInfo{
			EventTime: pb.MessageInfo.EventTime.AsTime(),
			IsLate:    pb.MessageInfo.IsLate,
		}
	}

	h.Kind = MessageKind(pb.Kind)

	if pb.Id != nil {
		h.ID = MessageID{
			VertexName: pb.Id.VertexName,
			Offset:     pb.Id.Offset,
			Index:      pb.Id.Index,
		}
	}

	h.Keys = pb.Keys
	h.Headers = pb.Headers

	return nil
}
