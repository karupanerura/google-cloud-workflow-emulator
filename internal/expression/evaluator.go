package expression

import (
	"fmt"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type Evaluator struct {
	SymbolTable types.SymbolTable
}

func (e *Evaluator) EvaluateValue(expr *Expr) (ret any, err error) {
	ret, err = expr.execute(e.SymbolTable)
	if err != nil {
		return
	}

	if ref, ok := ret.(Reference); ok {
		v, err := ref.ResolveValue(e.SymbolTable)
		if err != nil {
			return nil, err
		}

		ret = v.Get()
	}

	return
}

func (e *Evaluator) EvaluateValueRecursive(value any) (any, error) {
	switch v := value.(type) {
	case *Expr:
		return e.EvaluateValue(v)

	case map[string]any:
		result := make(map[string]any, len(v))
		for key, value := range v {
			var err error
			result[key], err = e.EvaluateValueRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("key=%q: %w", key, err)
			}
		}
		return result, nil

	case []any:
		result := make([]any, len(v))
		for i, value := range v {
			var err error
			result[i], err = e.EvaluateValueRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("index=%d: %w", i, err)
			}
		}
		return result, nil

	default:
		return value, nil
	}
}

func (e *Evaluator) ResolveReference(expr *Expr) (Reference, error) {
	ret, err := expr.execute(e.SymbolTable)
	if err != nil {
		return nil, err
	}

	ref, ok := ret.(Reference)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid reference", expr.Source)
	}

	return ref, nil
}

func (e *Evaluator) ResolveReferenceRecursive(value any) (any, error) {
	switch v := value.(type) {
	case *Expr:
		return e.ResolveReference(v)

	case map[string]any:
		result := make(map[string]any, len(v))
		for key, value := range v {
			var err error
			result[key], err = e.ResolveReferenceRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("key=%q: %w", key, err)
			}
		}
		return result, nil

	case []any:
		result := make([]any, len(v))
		for i, value := range v {
			var err error
			result[i], err = e.ResolveReferenceRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("index=%d: %w", i, err)
			}
		}
		return result, nil

	default:
		return value, nil
	}
}
