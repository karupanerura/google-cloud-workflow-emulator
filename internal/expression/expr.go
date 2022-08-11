package expression

import (
	"fmt"
	"strings"
)

type Expr struct {
	Source string
	operation
}

func (e *Expr) IsSymbol() (ok bool) {
	_, ok = e.operation.(*retrieveSymbolOperation)
	return ok
}

func (e *Expr) IsField() (ok bool) {
	_, ok = e.operation.(*retrieveFieldOperation)
	return ok
}

func (e *Expr) CanReference() (ok bool) {
	switch e.operation.(type) {
	case *retrieveFieldOperation, *retrieveSymbolOperation:
		return true
	default:
		return false
	}
}

func (e *Expr) String() string {
	return e.Source
}

func ExpandExprRecursive(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return ExpandExpr(v)

	case map[string]any:
		result := make(map[string]any, len(v))
		for key, value := range v {
			var err error
			result[key], err = ExpandExprRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("key=%q: %w", key, err)
			}
		}
		return result, nil

	case []any:
		result := make([]any, len(v))
		for i, value := range v {
			var err error
			result[i], err = ExpandExprRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("index=%d: %w", i, err)
			}
		}
		return result, nil

	default:
		return value, nil
	}
}

// refs. https://cloud.google.com/workflows/docs/reference/syntax/expressions
func ExpandExpr(str string) (any, error) {
	if IsExpr(str) {
		return ParseExpr(TrimExprParen(str))
	}

	return str, nil
}

func IsExpr(str string) bool {
	return strings.HasPrefix(str, "${") && strings.HasSuffix(str, "}")
}

func TrimExprParen(str string) string {
	return strings.TrimSuffix(strings.TrimPrefix(str, "${"), "}")
}
