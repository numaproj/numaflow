package util

import "encoding/json"

func MustJSON(in interface{}) string {
	if data, err := json.Marshal(in); err != nil {
		panic(err)
	} else {
		return string(data)
	}
}

// MustUnJSON unmarshalls JSON or panics.
// v - must be []byte or string
// in - must be a pointer.
func MustUnJSON(v interface{}, in interface{}) {
	switch data := v.(type) {
	case []byte:
		if err := json.Unmarshal(data, in); err != nil {
			panic(err)
		}
	case string:
		MustUnJSON([]byte(data), in)
	default:
		panic("unknown type")
	}
}
