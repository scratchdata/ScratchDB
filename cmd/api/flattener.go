package api

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

func parseMap(obj map[string]interface{}, path []string, useIndices bool) [][]map[string]interface{} {
	var result [][]map[string]interface{}
	for k, v := range obj {
		result = append(result, Flatten(v, append(path, k), useIndices))
	}
	return result
}

func crossProduct(dicts [][]map[string]interface{}) []map[string]interface{} {
	if len(dicts) == 0 {
		return []map[string]interface{}{{}}
	}
	var result []map[string]interface{}
	for _, lhs := range dicts[0] {
		for _, rhs := range crossProduct(dicts[1:]) {
			result = append(result, merge(lhs, rhs))
		}
	}
	return result
}

func merge(lhs, rhs map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range lhs {
		result[k] = v
	}
	for k, v := range rhs {
		result[k] = v
	}
	return result
}

func pathToString(path []string) string {
	var result string
	for i, p := range path {
		if i > 0 {
			result += "_"
		}
		result += p
	}
	return result
}

func Flatten(obj interface{}, path []string, useIndices bool) []map[string]interface{} {
	switch obj := obj.(type) {
	case []interface{}:
		if len(obj) > 0 {
			if useIndices {
				var result []map[string]interface{}
				for i, item := range obj {
					result = append(result, Flatten(item, append(path, strconv.Itoa(i)), useIndices)...)
				}
				return result
			}
			var result []map[string]interface{}
			for _, item := range obj {
				result = append(result, Flatten(item, path, useIndices)...)
			}
			return result
		} else {
			return []map[string]interface{}{{
				pathToString(path): nil,
			}}
		}
	case map[string]interface{}:
		return crossProduct(parseMap(obj, path, useIndices))
	default:
		return []map[string]interface{}{{
			pathToString(path): obj,
		}}
	}
}

func FlattenJSON(obj string, path []string, useIndices bool) ([]string, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(obj), &data)
	if err != nil {
		return nil, err
	}

	flattened := Flatten(data, path, useIndices)

	rc := make([]string, 0)
	for _, f := range flattened {
		b, err := json.Marshal(f)
		if err != nil {
			log.Err(err).Msg("failed to marshal json")
			return nil, err
		}
		rc = append(rc, string(b))
	}

	return rc, nil
}

// ExplodeJSON expands a JSON object. It returns the exploded objects and an error.
// ExplodeJSON returns a non-nil error when the object does not exist.
// It may return partial values and a non-mil error if some objects are parseable.
func ExplodeJSON(o gjson.Result) ([]string, error) {
	if !o.Exists() {
		return nil, errors.New("cannot explode invalid object")
	}
	var (
		lines []string
		err   error
	)

	doFlat := func(v string) {
		flats, flatErr := FlattenJSON(v, nil, false)
		if err != nil {
			err = errors.Join(err, flatErr)
		}
		lines = append(lines, flats...)
	}

	if o.IsArray() {
		for _, item := range o.Array() {
			doFlat(item.Raw)
		}
	} else {
		doFlat(o.Raw)
	}

	return lines, nil
}
