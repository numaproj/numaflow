package util

import (
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	error2 "github.com/numaproj/numaflow/pkg/sdkclient/error"
)

func ToUDFErr(name string, err error) error {
	if err == nil {
		return nil
	}
	statusCode, ok := status.FromError(err)
	// default udfError
	udfError := error2.New(error2.NonRetryable, statusCode.Message())
	// check if it's a standard status code
	if !ok {
		// if not, the status code will be unknown which we consider as non retryable
		// return default udfError
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
	switch statusCode.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.Unknown:
		// update to retryable err
		udfError = error2.New(error2.Retryable, statusCode.Message())
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	default:
		log.Printf("failed %s: %s", name, udfError.Error())
		return udfError
	}
}
