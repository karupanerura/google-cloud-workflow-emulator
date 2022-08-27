package defaults

import (
	"fmt"
	"math"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Math = aggregateFunctionsToMap("math", []types.Function{
	types.MustNewFunction("math.abs", []types.Argument{
		{Name: "x"},
	}, func(x any) (any, error) {
		switch n := x.(type) {
		case int64:
			if n == math.MinInt64 {
				return nil, &types.Error{
					Tag: types.ValueErrorTag,
					Err: fmt.Errorf("x is MinInt64: %v", x),
				}
			}
			return int64(math.Abs(float64(n))), nil

		case float64:
			return math.Abs(n), nil

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("x is not an integer or floating-point number: %v", x),
			}
		}
	}),
	types.MustNewFunction("math.max", []types.Argument{
		{Name: "x"},
		{Name: "y"},
	}, func(x, y any) (any, error) {
		switch n := x.(type) {
		case int64:
			x := n
			switch n := y.(type) {
			case int64:
				y := n
				if x > y {
					return x, nil
				}
				return y, nil

			case float64:
				y := n
				return math.Max(float64(x), y), nil

			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("y is not an integer or floating-point number: %v", x),
				}
			}

		case float64:
			x := n
			switch n := y.(type) {
			case int64:
				y := n
				return math.Max(x, float64(y)), nil

			case float64:
				y := n
				return math.Max(x, y), nil

			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("y is not an integer or floating-point number: %v", x),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("x is not an integer or floating-point number: %v", x),
			}
		}
	}),
	types.MustNewFunction("math.min", []types.Argument{
		{Name: "x"},
		{Name: "y"},
	}, func(x, y any) (any, error) {
		switch n := x.(type) {
		case int64:
			x := n
			switch n := y.(type) {
			case int64:
				y := n
				if x < y {
					return x, nil
				}
				return y, nil

			case float64:
				y := n
				return math.Min(float64(x), y), nil

			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("y is not an integer or floating-point number: %v", x),
				}
			}

		case float64:
			x := n
			switch n := y.(type) {
			case int64:
				y := n
				return math.Min(x, float64(y)), nil

			case float64:
				y := n
				return math.Min(x, y), nil

			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("y is not an integer or floating-point number: %v", x),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("x is not an integer or floating-point number: %v", x),
			}
		}
	}),
})
