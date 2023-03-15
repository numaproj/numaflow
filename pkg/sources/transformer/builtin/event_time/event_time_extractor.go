/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package eventtime

import (
	"context"
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"

	"github.com/numaproj/numaflow/pkg/shared/expr"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type eventTimeExtractor struct {
	// expression is used to extract the string representation of the event time from message payload.
	// e.g. `json(payload).metadata.time`
	expression string
	// format specifies the layout of extracted time string.
	// with format, eventTimeExtractor uses the time.Parse function to translate the event time string representation to time.Time object.
	// otherwise if format is not specified, eventTimeExtractor uses dateparse to find format based on the time string.
	format string
}

func New(args map[string]string) (functionsdk.MapTFunc, error) {
	expr, existing := args["expression"]
	if !existing {
		return nil, fmt.Errorf(`missing "expression"`)
	}

	var format string
	if format, existing = args["format"]; !existing {
		format = ""
	}

	e := eventTimeExtractor{
		expression: expr,
		format:     format,
	}

	return func(ctx context.Context, key string, datum functionsdk.Datum) functionsdk.MessageTs {
		log := logging.FromContext(ctx)
		resultMsg, err := e.apply(datum.EventTime(), datum.Value())
		if err != nil {
			log.Warnf("event time extractor got an error: %v, skip updating event time...", err)
		}
		return functionsdk.MessageTsBuilder().Append(resultMsg)
	}, nil
}

// apply compiles the payload to extract the new event time. If there is any error during extraction,
// we pass on the original input event time. Otherwise, we assign the new event time to the message.
func (e eventTimeExtractor) apply(et time.Time, payload []byte) (functionsdk.MessageT, error) {
	timeStr, err := expr.EvalStr(e.expression, payload)
	if err != nil {
		return functionsdk.MessageTToAll(et, payload), err
	}

	var newEventTime time.Time
	time.Local, _ = time.LoadLocation("UTC")
	if e.format != "" {
		newEventTime, err = time.Parse(e.format, timeStr)
	} else {
		newEventTime, err = dateparse.ParseStrict(timeStr)
	}
	if err != nil {
		return functionsdk.MessageTToAll(et, payload), err
	} else {
		return functionsdk.MessageTToAll(newEventTime, payload), nil
	}
}
