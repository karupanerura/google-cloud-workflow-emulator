package defaults

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var ExpressionHelpers = aggregateFunctionsToSymbolTable(
	types.MustNewFunction("double", []types.Argument{
		{Name: "attribute"},
	}, func(attribute any) (float64, error) {
		switch v := attribute.(type) {
		case string:
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, &types.Error{
					Tag: types.ValueErrorTag,
					Err: fmt.Errorf("cannot convert to a floating-number: %w", err),
				}
			}

			return vv, nil

		case int64:
			return float64(v), nil

		case float64:
			return v, nil

		default:
			return 0, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("the attribute is not a number or string: %v", v),
			}
		}
	}),
	types.MustNewFunction("int", []types.Argument{
		{Name: "attribute"},
	}, func(attribute any) (int64, error) {
		switch v := attribute.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, &types.Error{
					Tag: types.ValueErrorTag,
					Err: fmt.Errorf("cannot convert to an integer: %w", err),
				}
			}

			return vv, nil

		case float64:
			return int64(v), nil

		case int64:
			return v, nil

		default:
			return 0, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("the attribute is not a number or string: %v", v),
			}
		}
	}),
	types.MustNewFunction("string", []types.Argument{
		{Name: "attribute"},
	}, func(attribute any) (string, error) {
		switch n := attribute.(type) {
		case int64:
			return strconv.FormatInt(n, 10), nil

		case float64:
			return strconv.FormatFloat(n, 'f', -1, 64), nil

		case bool:
			return strconv.FormatBool(n), nil

		default:
			return "", &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("attribute is not a number or boolean: %v", attribute),
			}
		}
	}),
	types.MustNewFunction("keys", []types.Argument{
		{Name: "attribute"},
	}, func(attribute map[string]any) (keys []string, err error) {
		for key := range attribute {
			keys = append(keys, key)
		}
		return
	}),
	types.MustNewFunction("len", []types.Argument{
		{Name: "attribute"},
	}, func(attribute any) (int64, error) {
		switch v := attribute.(type) {
		case string:
			return int64(utf8.RuneCountInString(v)), nil
		case []any:
			return int64(len(v)), nil
		case map[string]any:
			return int64(len(v)), nil
		default:
			return 0, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("attribute is not a string, array or map: %v", attribute),
			}
		}
	}),
)
