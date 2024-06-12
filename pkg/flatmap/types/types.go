package types

import (
	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"

	"github.com/numaproj/numaflow/pkg/isb"
)

// TODO(stream): check what all data to keep here, this might be a lot to keep the whole message?
// We might just need only a few details out of these
type ResponseFlatmap struct {
	ParentMessage *isb.ReadMessage
	Uid           string
	RespMessage   *flatmappb.MapResponse
	AckIt         bool
	Total         int64
}

type WriteMsgFlatmap struct {
	Message *ResponseFlatmap
	AckIt   bool
}

type AckMsgFlatmap struct {
	Message *isb.ReadMessage
	AckIt   bool
}

type RequestFlatmap struct {
	Request    *isb.ReadMessage
	Uid        string
	ReadOffset isb.Offset
}
