package rpc

import (
	context2 "context"
	"fmt"
	"time"

	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/flatmap/tracker"
	"github.com/numaproj/numaflow/pkg/flatmap/types"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/flatmapper"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedFlatmap is a flat map applier that uses gRPC client to invoke the flat map UDF.
// It implements the applier.FlatmapApplier interface.
type GRPCBasedFlatmap struct {
	client     flatmapper.Client
	tracker    *tracker.Tracker
	vertexName string
}

func NewUDSgRPCBasedFlatmap(client flatmapper.Client, vertexName string) *GRPCBasedFlatmap {
	return &GRPCBasedFlatmap{client: client, tracker: tracker.NewTracker(), vertexName: vertexName}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedFlatmap) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedFlatmap) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the reduce udf is connected.
func (u *GRPCBasedFlatmap) WaitUntilReady(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				logger.Infof("waiting for reduce udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// ApplyMap applies the map udf on the stream of read messages and streams the responses back on the responseCh
// Internally, it spawns two go-routines, one for sending the requests to the client and the other to listen to the
// responses back from it.
func (u *GRPCBasedFlatmap) ApplyMap(ctx context2.Context, messageStream []*types.RequestFlatmap, responseCh chan<- *flatmappb.MapResponse) (chan struct{}, <-chan error) {
	// errCh is used to propagate any errors recieved from the grpc client upstream so that they can be handled
	// accordingly.
	errCh := make(chan error)
	doneChan := make(chan struct{})

	// flatmapRequests is a channel on which the input requests are streamed, this is then consumed by the grpc client
	//TODO(stream): do we need to keep this buffered?
	flatmapRequests := make(chan *flatmappb.MapRequest)

	// Response routine:
	// This routine would invoke the MapFn from the client and then keep listening to the response and errorCh
	// from the same.
	// On getting a response, it would parse it to check whether this is the last response for a given request,
	// in such a case we will remove it from the tracker.
	// It would also look for any errors received from the client, and then propagate them.
	// TODO(stream): should we move this tracking mechanism on a higher layer, and track a request till the
	//  end of lifetime ie ack
	go func() {
		// TODO(stream): Instead of closing the channel here, return a done and close this upstream?
		// close the responseCh while exiting to indicate downstream that no more responses expected from
		// gRPC
		defer close(doneChan)
		// invoke the MapFn from the gRPC client for a stream of input requests
		// resultCh -> chan to read responses streamed back
		// mapErrCh -> chan for reading any errors encountered during gRPC
		resultCh, mapErrCh := u.client.MapFn(ctx, flatmapRequests)
		// Keep running forever until explicit return
		for {
			// See if we got a response from the client, could be on the response or the error channel
			select {
			// Got a response on the resultCh
			case result, ok := <-resultCh:
				// If there are no more messages to read on the stream, or a nil message we can safely assume that
				// gRPC has no more messages to send. Hence, we can return from here
				if !ok || result == nil {
					return
				}
				//resp, remove, uid := u.parseMapResponse(result)
				//// If this was the last response for a request, let's remove from the tracker
				//// As this is a special message indicates that all responses for a request have been
				//// received, the ackIt field is enabled to indicate that the parent request can be acked now.
				//if remove {
				//	u.tracker.RemoveRequest(uid)
				//}
				// Forward the received response to the channel
				//log.Print("MYDEBUG SENDING TO WRITER ", result.Result.GetUuid())
				responseCh <- result
			case err := <-mapErrCh:
				// We got a context done while processing the gRPC, hence stop processing
				// The specific case for ctx.Done() is already checked in MapFn
				if err == ctx.Err() {
					errCh <- err
					return
				}
				// If we got any other type of error
				if err != nil {
					// TODO(stream): graceful handling of error, so that we can drain all the
					//  unprocessed messaged and retry them again. Once way could be to just restart the NUMA container
					//  in such a case, which would force a reread of the messages which have not been acked.
					errCh <- convertToUdfError(err)
					// TODO(stream): Should we stop processing further in this case then
					return
				}
			}
		}
	}()

	// Read routine: this goroutine reads on the messageStream slice and sends each
	// of the read messages to the grpc client
	// after transforming it to a MapRequest. Once all messages are sent, it closes the input channel
	// to indicate that all requests have been read.
	// On creating a new request, we add it to a tracker map so that the responses on the stream
	// can be mapped backed to the given parent request
	go func() {
		defer close(flatmapRequests)
		for _, req := range messageStream {
			d := u.createMapRequest(req)
			//log.Print("MYDEBUG SENDING TO client ", d.GetUuid())
			flatmapRequests <- d
		}
	}()
	return doneChan, errCh
}

// createMapRequest takes an isb.ReadMessage and returns proto MapRequest
func (u *GRPCBasedFlatmap) createMapRequest(reqMsg *types.RequestFlatmap) *flatmappb.MapRequest {
	msg := reqMsg.Request
	keys := msg.Keys
	payload := msg.Body.Payload
	//// Add the request to the tracker, and get the unique UUID corresponding to it
	//uid := u.tracker.AddRequest(msg)
	// Create the MapRequest, with the required fields.
	var d = &flatmappb.MapRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(msg.EventTime),
		Watermark: timestamppb.New(msg.Watermark),
		Headers:   msg.Headers,
		Uuid:      reqMsg.Uid,
	}
	return d
}

// parseMapResponse takes a proto response from the gRPC and converts this into a ResponseFlatmap type,
// this also checks if this was a special EOR response, in such a case we indicate that the request corresponding
// to the response can be safely removed from the tracker.
func (u *GRPCBasedFlatmap) parseMapResponse(resp *flatmappb.MapResponse) (parsedResp *types.ResponseFlatmap, requestDone bool, uid string) {
	result := resp.Result
	eor := result.GetEOR()
	uid = result.GetUuid()
	//parentRequest, ok := u.tracker.GetRequest(uid)
	// TODO(stream): check what should be path for !ok, which means that we got a UUID
	// which has already been deleted from the tracker/ or never added in the first place
	// can this even happen though if messages are ordered and we only have a single routine processing it?
	//if !ok {
	//}
	// Request has completed remove from the tracker module
	if eor == true {
		return &types.ResponseFlatmap{
			//ParentMessage: parentRequest,
			Uid:         uid,
			RespMessage: nil,
			AckIt:       true,
			Total:       int64(resp.Result.Total),
		}, true, uid
	}
	//keys := result.GetKeys()
	//taggedMessage := &isb.WriteMessage{
	//	Message: isb.Message{
	//		Header: isb.Header{
	//			MessageInfo: parentRequest.MessageInfo,
	//			// We need this to be unique so that the ISB can execute its Dedup logic
	//			// this ID should be such that even when the same response is processed and received
	//			// again from the UDF, we still assign it the same ID.
	//			// The ID here will be a concat of the three values
	//			// parentRequest.ReadOffset - vertexName - result.Index
	//			//
	//			// ReadOffset - Will be the read offset of the request which corresponds to this response.
	//			// We have this stored in our tracker.
	//			//
	//			// VertexName - the name of the vertex from which this response is generated, this is
	//			// important to ensure that we can differentiate between messages emitted from 2 map vertices
	//			//
	//			// Result Index - This parameter is added on the SDK side.
	//			// We add the index of the message from the messages slice to the individual response.
	//			// TODO(stream): explore if there can be more robust ways to do this
	//			ID:   fmt.Sprintf("%s-%s-%s", parentRequest.ReadOffset.String(), u.vertexName, result.GetIndex()),
	//			Keys: keys,
	//		},
	//		Body: isb.Body{
	//			Payload: result.GetValue(),
	//		},
	//	},
	//	Tags: result.GetTags(),
	//}
	return &types.ResponseFlatmap{
		//ParentMessage: parentRequest,
		Uid: uid,
		//RespMessage: result,
		AckIt: false,
		Total: int64(resp.Result.Total),
	}, false, uid
}

// convertToUdfError converts the error returned by the MapFn to ApplyUDFErr
func convertToUdfError(err error) error {
	udfErr, _ := sdkerr.FromError(err)
	switch udfErr.ErrorKind() {
	case sdkerr.Retryable:
		// TODO: currently we don't handle retryable errors yet
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	case sdkerr.NonRetryable:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	case sdkerr.Canceled:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    context.Canceled.Error(),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	default:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}
}
