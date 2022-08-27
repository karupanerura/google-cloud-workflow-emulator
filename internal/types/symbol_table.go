package types

import (
	"sync"

	"github.com/samber/lo"
)

const (
	// internal symbols
	InternalInheritedVariablesSymbol = "__INTERNAL_INHERITED_VARIABLE_SET"
)

type InternalInheritedVariables struct {
	Shared map[string]bool
}

type SharedVariable struct {
	sync.RWMutex
	Value any
}

type SymbolTable map[string]any

func (st SymbolTable) Inherit(parent SymbolTable) SymbolTable {
	dst := make(SymbolTable, len(st))
	return lo.Assign(dst, parent, st)
}

func (st SymbolTable) ShallowClone() SymbolTable {
	dst := make(SymbolTable, len(st))
	return lo.Assign(dst, st)
}
