package types

import "github.com/numaproj/numaflow/pkg/isb"

// TODO(stream): check what all data to keep here, this might be a lot to keep the whole message?
type ResponseFlatmap struct {
	ParentMessage *isb.ReadMessage
	Uid           string
	RespMessage   *isb.WriteMessage
}

type WriteMsgFlatmap struct {
	Message *ResponseFlatmap
	AckIt   bool
}

type AckMsgFlatmap struct {
	Message *isb.ReadMessage
	AckIt   bool
}
