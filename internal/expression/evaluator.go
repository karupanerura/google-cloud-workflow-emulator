package expression

import (
	"fmt"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type Evaluator struct {
	SymbolTable *types.SymbolTable
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

func (e *Evaluator) LockSharedVariablesIfNeeded(exprs ...*Expr) (func(), error) {
	inheritedVariablesAny, ok := e.SymbolTable.Get(types.InternalInheritedVariablesSymbol)
	if !ok {
		return func() {}, nil
	}
	inheritedVariables := inheritedVariablesAny.(*types.InternalInheritedVariables)

	unlockers := make([]func(), 0, len(exprs))
	for _, expr := range exprs {
		ref, err := e.ResolveReference(expr)
		if err != nil {
			return nil, err
		}

		variable, err := ref.ResolveVariable(e.SymbolTable)
		if err != nil {
			return nil, err
		}

		rootSym, _ := variable.Paths()
		if inheritedVariables.Shared[rootSym] {
			v, ok := e.SymbolTable.Get(rootSym)
			if !ok {
				panic(fmt.Sprintf("assertion failure: not found shared variable=%q", rootSym))
			}

			sharedVar := v.(*types.SharedVariable)
			sharedVar.Lock()
			e.SymbolTable.Set(rootSym, sharedVar.Value)
			unlockers = append(unlockers, func() {
				sharedVar.Value, _ = e.SymbolTable.Get(rootSym)
				e.SymbolTable.Set(rootSym, sharedVar)
				sharedVar.Unlock()
			})
		}
	}
	if len(unlockers) == 0 {
		return func() {}, nil
	}
	if len(unlockers) == 1 {
		return unlockers[0], nil
	}

	return func() {
		for i := range unlockers {
			unlockers[len(unlockers)-i-1]() // unlock by reversed order
		}
	}, nil
}
