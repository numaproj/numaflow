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
	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"

	"github.com/numaproj/numaflow/pkg/shared/expr"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type expressions struct {
	filterExpr      string
	eventTimeExpr   string
	eventTimeFormat string
}

func New(args map[string]string) (sourcetransformer.SourceTransformFunc, error) {

	filterExpr, existing := args["filterExpr"]
	if !existing {
		return nil, fmt.Errorf(`missing "filterExpr"`)
	}

	eventTimeExpr, existing := args["eventTimeExpr"]
	if !existing {
		return nil, fmt.Errorf(`missing "eventTimeExpr"`)
	}

	var eventTimeFormat string
	if eventTimeFormat, existing = args["eventTimeFormat"]; !existing {
		eventTimeFormat = ""
	}

	e := expressions{
		filterExpr:      filterExpr,
		eventTimeExpr:   eventTimeExpr,
		eventTimeFormat: eventTimeFormat,
	}

	return func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
		log := logging.FromContext(ctx)
		resultMsg, err := e.apply(datum.EventTime(), datum.Value(), keys)
		if err != nil {
			log.Errorf("Filter or event time extractor got an error: %v", err)
		}
		return sourcetransformer.MessagesBuilder().Append(resultMsg)
	}, nil

}

func (e expressions) apply(et time.Time, payload []byte, keys []string) (sourcetransformer.Message, error) {
	result, err := expr.EvalBool(e.filterExpr, payload)
	if err != nil {
		return sourcetransformer.MessageToDrop(et), err
	}
	if result {
		timeStr, err := expr.EvalStr(e.eventTimeExpr, payload)
		if err != nil {
			return sourcetransformer.NewMessage(payload, et).WithKeys(keys), err
		}
		var newEventTime time.Time
		time.Local, _ = time.LoadLocation("UTC")
		if e.eventTimeFormat != "" {
			newEventTime, err = time.Parse(e.eventTimeFormat, timeStr)
		} else {
			newEventTime, err = dateparse.ParseStrict(timeStr)
		}
		if err != nil {
			return sourcetransformer.NewMessage(payload, et).WithKeys(keys), err
		} else {
			return sourcetransformer.NewMessage(payload, newEventTime).WithKeys(keys), nil
		}
	}
	return sourcetransformer.MessageToDrop(et), nil
}
