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

package timeextractionfilter

import (
	"context"
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/shared/expr"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type expressions struct {
	filterExpr    string
	eventTimeExpr string
	format        string
}

func New(args map[string]string) (functionsdk.MapTFunc, error) {

	filterExpr, existing := args["filterExpr"]
	if !existing {
		return nil, fmt.Errorf(`missing "filterExpr"`)
	}

	eventTimeExpr, existing := args["eventTimeExpr"]
	if !existing {
		return nil, fmt.Errorf(`missing "eventTimeExpr"`)
	}

	var format string
	if format, existing = args["format"]; !existing {
		format = ""
	}

	e := expressions{
		filterExpr:    filterExpr,
		eventTimeExpr: eventTimeExpr,
		format:        format,
	}

	return func(ctx context.Context, keys []string, datum functionsdk.Datum) functionsdk.MessageTs {
		log := logging.FromContext(ctx)
		resultMsg, err := e.apply(datum.EventTime(), datum.Value())
		if err != nil {
			log.Errorf("Filter or event time extractor got an error: %v", err)
		}
		return functionsdk.MessageTsBuilder().Append(resultMsg)
	}, nil

}

func (e expressions) apply(et time.Time, payload []byte) (functionsdk.MessageT, error) {
	result, err := expr.EvalBool(e.filterExpr, payload)
	if err != nil {
		return functionsdk.MessageTToDrop(), err
	}
	if result {
		timeStr, err := expr.EvalStr(e.eventTimeExpr, payload)
		if err != nil {
			return functionsdk.NewMessageT(payload, et), err
		}
		var newEventTime time.Time
		time.Local, _ = time.LoadLocation("UTC")
		if e.format != "" {
			newEventTime, err = time.Parse(e.format, timeStr)
		} else {
			newEventTime, err = dateparse.ParseStrict(timeStr)
		}
		if err != nil {
			return functionsdk.NewMessageT(payload, et), err
		} else {
			return functionsdk.NewMessageT(payload, newEventTime), nil
		}
	}
	return functionsdk.MessageTToDrop(), nil
}
