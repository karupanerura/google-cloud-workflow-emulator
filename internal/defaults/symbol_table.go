package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var DefaultSymbolTable = &types.SymbolTable{
	Symbols: map[string]any{
		"base64": Base64,
		"events": Events,
		"http":   HTTP,
		"json":   JSON,
		"list":   List,
		"map":    Map,
		"math":   Math,
		"retry":  Retry,
		"sys":    Sys,
		"text":   Text,
		"time":   Time,
	},
	ReadOnly: true,
	Parent:   ExpressionHelpers,
}
