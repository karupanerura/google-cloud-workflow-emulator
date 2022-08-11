package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func unmarshalJSONUseNumber(b []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(v)
}

func decodeJSONNumberRecursive(v any) (any, error) {
	switch vv := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(vv))
		for key, value := range vv {
			var err error
			m[key], err = decodeJSONNumberRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
		}
		return m, nil

	case []any:
		s := make([]any, len(vv))
		for i, value := range vv {
			var err error
			s[i], err = decodeJSONNumberRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return s, nil

	case json.Number:
		return decodeJSONNumber(vv)

	default:
		return v, nil
	}
}

func decodeJSONNumber(n json.Number) (any, error) {
	if i := strings.IndexByte(n.String(), '.'); i == -1 {
		if n, err := n.Int64(); errors.Is(err, strconv.ErrSyntax) {
			// retry parse as float64
		} else {
			return n, err
		}
	}
	return n.Float64()
}
