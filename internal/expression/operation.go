package expression

import (
	"fmt"
	"math"
	"reflect"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type operation interface {
	execute(*types.SymbolTable) (any, error)
}

var nullLiteralOperation = nullLiteralOperationTyp{}

type nullLiteralOperationTyp struct{}

func (s nullLiteralOperationTyp) execute(*types.SymbolTable) (any, error) {
	return nil, nil
}

type valueOperation[T any] struct {
	value T
}

func (s *valueOperation[T]) execute(*types.SymbolTable) (any, error) {
	return s.value, nil
}

type stringLiteralOperation = valueOperation[string]
type booleanLiteralOperation = valueOperation[bool]
type int64LiteralOperation = valueOperation[int64]
type float64LiteralOperation = valueOperation[float64]

type retrieveSymbolOperation struct {
	name string
}

func (s *retrieveSymbolOperation) execute(*types.SymbolTable) (any, error) {
	return &symbolReference{name: s.name}, nil
}

type retrieveFieldOperation struct {
	context operation
	field   operation
}

func (s *retrieveFieldOperation) execute(st *types.SymbolTable) (any, error) {
	rawContext, err := s.context.execute(st)
	if err != nil {
		return nil, fmt.Errorf("invalid context: %w", err)
	}

	rawField, err := s.field.execute(st)
	if err != nil {
		return nil, fmt.Errorf("invalid field: %w", err)
	}

	context, ok := rawContext.(Reference)
	if !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("retrive field %v: unexpected context type %T", rawField, rawContext),
		}
	}

	switch field := rawField.(type) {
	case string:
		return &fieldReference{context: context, name: field}, nil

	case int64:
		if field < 0 {
			return nil, &types.Error{
				Tag: types.IndexErrorTag,
				Err: fmt.Errorf("retrive field %v: array index %d out of bounds", rawField, field),
			}
		}
		return &indexReference{context: context, index: field}, nil

	default:
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("retrive field %v: unexpected context type %T", rawField, rawContext),
		}
	}
}

type calculateUnaryOperation struct {
	operator string
	value    operation
}

func (s *calculateUnaryOperation) execute(st *types.SymbolTable) (any, error) {
	value, err := s.value.execute(st)
	if err != nil {
		return nil, fmt.Errorf("value of unary operator %q: %w", s.operator, err)
	}
	if ref, ok := value.(Reference); ok {
		v, err := ref.ResolveValue(st)
		if err != nil {
			return nil, fmt.Errorf("value of unary operator %q: %w", s.operator, err)
		}
		value = v.Get()
	}

	switch s.operator {
	case "not":
		if v, ok := value.(bool); ok {
			return !v, nil
		}
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("unknown value type for unary operator %q: %T", s.operator, value),
		}
	case "+":
		switch value.(type) {
		case int64, float64:
			return value, nil
		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown value type for unary operator %q: %T", s.operator, value),
			}
		}
	case "-":
		switch v := value.(type) {
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown value type for unary operator %q: %T", s.operator, value),
			}
		}
	default:
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("unknown unary operator: %q for type %T", s.operator, value),
		}
	}
}

var nilableTypeSet = map[reflect.Kind]bool{
	reflect.Interface: true,
	reflect.Ptr:       true,
	reflect.Map:       true,
	reflect.Slice:     true,
	reflect.Chan:      true,
	reflect.Func:      true,
}

type calculateBinaryOperation struct {
	operator string
	left     operation
	right    operation
}

func (s *calculateBinaryOperation) execute(st *types.SymbolTable) (any, error) {
	left, err := s.left.execute(st)
	if err != nil {
		return nil, fmt.Errorf("left of operator %q: %w", s.operator, err)
	}
	if ref, ok := left.(Reference); ok {
		v, err := ref.ResolveValue(st)
		if err != nil {
			return nil, fmt.Errorf("left of operator %q: %w", s.operator, err)
		}
		left = v.Get()
	}

	right, err := s.right.execute(st)
	if err != nil {
		return nil, fmt.Errorf("right of operator %q: %w", s.operator, err)
	}
	if ref, ok := right.(Reference); ok {
		v, err := ref.ResolveValue(st)
		if err != nil {
			return nil, fmt.Errorf("right of operator %q: %w", s.operator, err)
		}
		right = v.Get()
	}

	// handle special NULL patterns for "==" and "!="
	// refs. https://cloud.google.com/workflows/docs/reference/syntax/datatypes#implicit-conversions
	if s.operator == "==" || s.operator == "!=" {
		leftIsNil := left == nil
		if !leftIsNil {
			if leftVal := reflect.ValueOf(left); nilableTypeSet[leftVal.Kind()] {
				leftIsNil = leftVal.IsNil()
			}
		}

		rightIsNil := right == nil
		if !rightIsNil {
			if rightVal := reflect.ValueOf(right); nilableTypeSet[rightVal.Kind()] {
				rightIsNil = rightVal.IsNil()
			}
		}

		if leftIsNil || rightIsNil {
			switch s.operator {
			case "==":
				return leftIsNil && rightIsNil, nil
			case "!=":
				return !(leftIsNil && rightIsNil), nil
			}
		}
	}

	// refs. https://cloud.google.com/workflows/docs/reference/syntax/datatypes#implicit-conversions
	switch lhs := left.(type) {
	case bool:
		switch rhs := right.(type) {
		case bool:
			switch s.operator {
			case "==":
				return lhs == rhs, nil
			case "!=":
				return lhs != rhs, nil
			case "or":
				return lhs || rhs, nil
			case "and":
				return lhs && rhs, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case []any:
			switch s.operator {
			case "in":
				for _, v := range rhs {
					if vv, ok := v.(bool); ok {
						if lhs == vv {
							return true, nil
						}
					}
				}
				return false, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown right value type of operator %q: %T", s.operator, left),
			}
		}

	case string:
		switch rhs := right.(type) {
		case string:
			switch s.operator {
			case "==":
				return lhs == rhs, nil
			case "!=":
				return lhs != rhs, nil
			case "+":
				return lhs + rhs, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case []any:
			switch s.operator {
			case "in":
				for _, v := range rhs {
					if vv, ok := v.(string); ok {
						if lhs == vv {
							return true, nil
						}
					}
				}
				return false, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case map[string]any:
			switch s.operator {
			case "in":
				_, found := rhs[lhs]
				return found, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown right value type of operator %q: %T", s.operator, right),
			}
		}

	case int64:
		switch rhs := right.(type) {
		case float64:
			switch s.operator {
			case "==":
				return float64(lhs) == rhs, nil
			case "!=":
				return float64(lhs) != rhs, nil
			case ">":
				return float64(lhs) > rhs, nil
			case ">=":
				return float64(lhs) >= rhs, nil
			case "<":
				return float64(lhs) < rhs, nil
			case "<=":
				return float64(lhs) <= rhs, nil
			case "+":
				return float64(lhs) + rhs, nil
			case "-":
				return float64(lhs) - rhs, nil
			case "*":
				return float64(lhs) * rhs, nil
			case "/":
				return float64(lhs) / rhs, nil
			case "//":
				return int64(math.Floor(float64(lhs) / rhs)), nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case int64:
			switch s.operator {
			case "==":
				return lhs == rhs, nil
			case "!=":
				return lhs != rhs, nil
			case ">":
				return lhs > rhs, nil
			case ">=":
				return lhs >= rhs, nil
			case "<":
				return lhs < rhs, nil
			case "<=":
				return lhs <= rhs, nil
			case "+":
				return lhs + rhs, nil
			case "-":
				return lhs - rhs, nil
			case "*":
				return lhs * rhs, nil
			case "/":
				return float64(lhs) / float64(rhs), nil
			case "//":
				return lhs / rhs, nil
			case "%":
				return lhs % rhs, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case []any:
			switch s.operator {
			case "in":
				for _, v := range rhs {
					if vv, ok := v.(int64); ok {
						if lhs == vv {
							return true, nil
						}
					}
				}
				return false, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown right value type of operator %q: %T", s.operator, right),
			}
		}

	case float64:
		switch rhs := right.(type) {
		case float64:
			switch s.operator {
			case "==":
				return lhs == rhs, nil
			case "!=":
				return lhs != rhs, nil
			case ">":
				return lhs > rhs, nil
			case ">=":
				return lhs >= rhs, nil
			case "<":
				return lhs < rhs, nil
			case "<=":
				return lhs <= rhs, nil
			case "+":
				return lhs + rhs, nil
			case "-":
				return lhs - rhs, nil
			case "*":
				return lhs * rhs, nil
			case "/":
				return lhs / rhs, nil
			case "//":
				return int64(math.Floor(lhs / rhs)), nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case int64:
			switch s.operator {
			case "==":
				return lhs == float64(rhs), nil
			case "!=":
				return lhs != float64(rhs), nil
			case ">":
				return lhs > float64(rhs), nil
			case ">=":
				return lhs >= float64(rhs), nil
			case "<":
				return lhs < float64(rhs), nil
			case "<=":
				return lhs <= float64(rhs), nil
			case "+":
				return lhs + float64(rhs), nil
			case "-":
				return lhs - float64(rhs), nil
			case "*":
				return lhs * float64(rhs), nil
			case "/":
				return lhs / float64(rhs), nil
			case "//":
				return int64(math.Floor(lhs / float64(rhs))), nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		case []any:
			switch s.operator {
			case "in":
				for _, v := range rhs {
					if vv, ok := v.(float64); ok {
						if lhs == vv {
							return true, nil
						}
					}
				}
				return false, nil
			default:
				return nil, &types.Error{
					Tag: types.TypeErrorTag,
					Err: fmt.Errorf("invalid operator %q for left=%T right=%T", s.operator, left, right),
				}
			}

		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("unknown right value type of operator %q: %T", s.operator, right),
			}
		}

	default:
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("unknown left value type of operator %q: %T", s.operator, left),
		}
	}
}

type callFunctionOperation struct {
	function operation
	args     []operation
}

func (s *callFunctionOperation) execute(st *types.SymbolTable) (any, error) {
	value, err := s.function.execute(st)
	if err != nil {
		return nil, err
	}

	var path string
	if ref, ok := value.(Reference); ok {
		v, err := ref.ResolveValue(st)
		if err != nil {
			return nil, err
		}
		path = v.Path()
		value = v.Get()
	} else {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("not a function: %+v", value),
		}
	}

	type function interface {
		Call([]any) (any, error)
	}
	f, ok := value.(function)
	if !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: not a function", path),
		}
	}

	args := make([]any, len(s.args))
	for i, arg := range s.args {
		var err error
		args[i], err = arg.execute(st)
		if err != nil {
			return nil, fmt.Errorf("%s args[%d]: %w", path, i, err)
		}

		if ref, ok := args[i].(Reference); ok {
			v, err := ref.ResolveValue(st)
			if err != nil {
				return nil, fmt.Errorf("%s args[%d]: %w", path, i, err)
			}
			args[i] = v.Get()
		}
	}

	ret, err := f.Call(args)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ret, nil
}
