package types

import (
	"fmt"
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

type SymbolTable struct {
	Symbols  map[string]any
	ReadOnly bool
	Parent   *SymbolTable
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		Symbols: map[string]any{},
	}
}

func (st *SymbolTable) Get(key string) (any, bool) {
	v, ok := st.Symbols[key]
	if ok {
		return v, true
	}
	if st.Parent != nil {
		return st.Parent.Get(key)
	}
	return nil, false
}

func (st *SymbolTable) Set(key string, value any) {
	if updated := st.set(key, value); updated {
		return
	}
	if st.ReadOnly {
		panic(fmt.Sprintf("Cannot assign %q=%+v to read only symbol table", key, value))
	}
	st.Symbols[key] = value
}

func (st *SymbolTable) set(key string, value any) bool {
	if !st.ReadOnly {
		_, ok := st.Symbols[key]
		if ok {
			st.Symbols[key] = value
			return true
		}
	}
	if st.Parent != nil {
		return st.Parent.set(key, value)
	}
	return false
}

func (st *SymbolTable) KeysChan() <-chan string {
	ch := make(chan string)
	go st.keysChan(ch)
	return ch
}

func (st *SymbolTable) keysChan(ch chan string) {
	if st.Parent != nil {
		st.Parent.keysChan(ch)
	}
	for key := range st.Symbols {
		ch <- key
	}
}

func (st *SymbolTable) ShallowClone() *SymbolTable {
	return &SymbolTable{
		Symbols:  lo.Assign(map[string]any{}, st.Symbols),
		ReadOnly: st.ReadOnly,
		Parent:   st.Parent,
	}
}
