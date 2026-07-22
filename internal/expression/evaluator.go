package expression

import (
	"fmt"
	"sort"

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

// LockSharedVariablesIfNeeded locks every shared variable statically referenced
// by the given values (each value may be an *Expr or a map/list containing them)
// for the duration of the current step. It must be called with the step's whole
// expression footprint (assignment targets and the expressions they read), so
// that a step never blocks on a shared variable while holding another one:
// all locks are acquired upfront in a single global (name-sorted) order.
//
// While a lock is held, a copy of the shared variable's value is installed into
// the evaluator's own (goroutine-local) symbol table leaf so that every read and
// write within the step resolves locally without touching the lock again
// (sync.RWMutex is not reentrant). The returned unlock function publishes the
// leaf value back into the shared variable, removes the local entry, and
// releases the locks in reverse order. Callers must defer it so the value is
// published even when the step fails halfway.
func (e *Evaluator) LockSharedVariablesIfNeeded(values ...any) (func(), error) {
	inheritedVariablesAny, ok := e.SymbolTable.Get(types.InternalInheritedVariablesSymbol)
	if !ok {
		return func() {}, nil
	}
	inheritedVariables := inheritedVariablesAny.(*types.InternalInheritedVariables)

	symbolSet := map[string]struct{}{}
	collectReferencedSymbolsRecursive(symbolSet, values...)

	roots := make([]string, 0, len(symbolSet))
	for sym := range symbolSet {
		if inheritedVariables.Shared[sym] {
			roots = append(roots, sym)
		}
	}
	sort.Strings(roots)

	unlockers := make([]func(), 0, len(roots))
	for _, root := range roots {
		v, ok := e.SymbolTable.Get(root)
		if !ok {
			continue
		}
		sharedVar, isShared := v.(*types.SharedVariable)
		if !isShared {
			// the name resolves to a goroutine-local variable that shadows the
			// shared one (e.g. a loop value variable), so no locking is needed
			continue
		}

		root := root
		sharedVar.Lock()
		e.SymbolTable.Symbols[root] = types.DeepCopyValue(sharedVar.Value)
		unlockers = append(unlockers, func() {
			sharedVar.Value = e.SymbolTable.Symbols[root]
			delete(e.SymbolTable.Symbols, root)
			sharedVar.Unlock()
		})
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

func collectReferencedSymbolsRecursive(set map[string]struct{}, values ...any) {
	for _, value := range values {
		switch v := value.(type) {
		case *Expr:
			v.AppendReferencedSymbols(set)
		case map[string]any:
			for _, value := range v {
				collectReferencedSymbolsRecursive(set, value)
			}
		case []any:
			for _, value := range v {
				collectReferencedSymbolsRecursive(set, value)
			}
		}
	}
}
