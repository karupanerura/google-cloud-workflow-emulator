package expression

// SymbolName returns the symbol name when the expression is a plain symbol reference.
func (e *Expr) SymbolName() (string, bool) {
	if op, ok := e.operation.(*retrieveSymbolOperation); ok {
		return op.name, true
	}
	return "", false
}

// AppendReferencedSymbols adds every symbol name statically referenced by the
// expression to set. Field and index accesses contribute their context's root
// symbol as well as any symbols used inside the accessor (e.g. a[i] contributes
// both a and i).
func (e *Expr) AppendReferencedSymbols(set map[string]struct{}) {
	collectSymbols(e.operation, set)
}

func collectSymbols(op operation, set map[string]struct{}) {
	switch v := op.(type) {
	case *retrieveSymbolOperation:
		set[v.name] = struct{}{}
	case *retrieveFieldOperation:
		collectSymbols(v.context, set)
		collectSymbols(v.field, set)
	case *calculateUnaryOperation:
		collectSymbols(v.value, set)
	case *calculateBinaryOperation:
		collectSymbols(v.left, set)
		collectSymbols(v.right, set)
	case *callFunctionOperation:
		collectSymbols(v.function, set)
		for _, arg := range v.args {
			collectSymbols(arg, set)
		}
	}
}
