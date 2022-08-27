package defaults

import (
	"fmt"
	"strings"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

func aggregateFunctionsToSymbolTable(funcs ...types.Function) types.SymbolTable {
	m := make(types.SymbolTable, len(funcs))
	for _, f := range funcs {
		name := f.Name()
		if _, duplicated := m[name]; duplicated {
			panic(fmt.Sprintf("duplicated function name: %s", name))
		}
		m[name] = f
	}
	return m
}

func aggregateFunctionsToMap(prefix string, funcs []types.Function) map[string]any {
	prefix += "."

	m := make(map[string]any, len(funcs))
	for _, f := range funcs {
		if !strings.HasPrefix(f.Name(), prefix) {
			panic(fmt.Sprintf("invalid prefix for function name: %s (expected to start with %q)", f.Name(), prefix))
		}

		name := strings.TrimPrefix(f.Name(), prefix)
		if _, duplicated := m[name]; duplicated {
			panic(fmt.Sprintf("duplicated function name: %s", name))
		}
		m[name] = f
	}
	return m
}

func mergeMaps(maps ...map[string]any) map[string]any {
	m := map[string]any{}
	for _, mm := range maps {
		for k, v := range mm {
			m[k] = v
		}
	}
	return m
}
