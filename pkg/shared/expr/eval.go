package expr

import (
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"github.com/antonmedv/expr"
	"strconv"
)

var sprigFuncMap = sprig.GenericFuncMap()

const root = "payload"

func EvalBool(expression string, msg []byte) (bool, error) {
	msgMap := map[string]interface{}{
		root: string(msg),
	}
	env := getFuncMap(msgMap)
	result, err := expr.Eval(expression, env)
	if err != nil {
		return false, fmt.Errorf("unable to evaluate expression '%s': %s", expression, err)
	}
	resultBool, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("unable to cast expression result '%s' to bool", result)
	}
	return resultBool, nil
}

func getFuncMap(m map[string]interface{}) map[string]interface{} {
	env := Expand(m)
	env["sprig"] = sprigFuncMap
	env["json"] = _json
	env["int"] = _int
	env["string"] = _string
	return env
}

func _int(v interface{}) int {
	switch w := v.(type) {
	case []byte:
		i, err := strconv.Atoi(string(w))
		if err != nil {
			panic(fmt.Errorf("cannot convert %q an int", v))
		}
		return i
	case string:
		i, err := strconv.Atoi(w)
		if err != nil {
			panic(fmt.Errorf("cannot convert %q to int", v))
		}
		return i
	case float64:
		return int(w)
	case int:
		return w
	default:
		panic(fmt.Errorf("cannot convert %q to int", v))
	}
}

func _string(v interface{}) string {
	switch w := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(w)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func _json(v interface{}) map[string]interface{} {
	x := make(map[string]interface{})
	switch w := v.(type) {
	case nil:
		return nil
	case []byte:
		if err := json.Unmarshal(w, &x); err != nil {
			panic(fmt.Errorf("cannot convert %q to object: %v", v, err))
		}
		return x
	case string:
		if err := json.Unmarshal([]byte(w), &x); err != nil {
			panic(fmt.Errorf("cannot convert %q to object: %v", v, err))
		}
		return x
	default:
		panic("unknown type")
	}
}
