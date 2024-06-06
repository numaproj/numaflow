package types

import "github.com/numaproj/numaflow/pkg/isb"

// TODO(stream): check what all data to keep here, this might be a lot to keep the whole message?
// We might just need only a few details out of these
type ResponseFlatmap struct {
	ParentMessage *isb.ReadMessage
	Uid           string
	RespMessage   *isb.WriteMessage
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
