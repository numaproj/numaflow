package httpexpect

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func canonNumber(opChain *chain, in interface{}) (out float64, ok bool) {
	ok = true
	defer func() {
		if err := recover(); err != nil {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{in},
				Errors: []error{
					errors.New("expected: valid number"),
					fmt.Errorf("%s", err),
				},
			})
			ok = false
		}
	}()
	out = reflect.ValueOf(in).Convert(reflect.TypeOf(float64(0))).Float()
	return
}

func canonArray(opChain *chain, in interface{}) ([]interface{}, bool) {
	var out []interface{}
	data, ok := canonValue(opChain, in)
	if ok {
		out, ok = data.([]interface{})
		if !ok {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{in},
				Errors: []error{
					errors.New("expected: valid array"),
				},
			})
		}
	}
	return out, ok
}

func canonMap(opChain *chain, in interface{}) (map[string]interface{}, bool) {
	var out map[string]interface{}
	data, ok := canonValue(opChain, in)
	if ok {
		out, ok = data.(map[string]interface{})
		if !ok {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{in},
				Errors: []error{
					errors.New("expected: valid map"),
				},
			})
		}
	}
	return out, ok
}

func canonValue(opChain *chain, in interface{}) (interface{}, bool) {
	b, err := json.Marshal(in)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{in},
			Errors: []error{
				errors.New("expected: marshalable value"),
				err,
			},
		})
		return nil, false
	}

	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{in},
			Errors: []error{
				errors.New("expected: unmarshalable value"),
				err,
			},
		})
		return nil, false
	}

	return out, true
}

func canonDecode(opChain *chain, value interface{}, target interface{}) {
	if target == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil target argument"),
			},
		})
		return
	}

	b, err := json.Marshal(value)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: marshable value"),
			},
		})
		return
	}

	if err := json.Unmarshal(b, target); err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{target},
			Errors: []error{
				errors.New("expected: value can be unmarshaled into target argument"),
			},
		})
		return
	}
}
