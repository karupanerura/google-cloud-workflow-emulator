package types

import "github.com/samber/lo"

type SymbolTable map[string]any

func (st SymbolTable) Inherit(parent SymbolTable) SymbolTable {
	dst := make(SymbolTable, len(st))
	return lo.Assign(dst, parent, st)
}

func (st SymbolTable) ShallowClone() SymbolTable {
	dst := make(SymbolTable, len(st))
	return lo.Assign(dst, st)
}
