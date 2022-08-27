package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var DefaultSymbolTable = ExpressionHelpers.Inherit(types.SymbolTable{
	"base64": Base64,
	"events": Events,
	"experimental": map[string]any{
		"executions": ExperimentalExecutions,
	},
	"http":  HTTP,
	"json":  JSON,
	"list":  List,
	"map":   Map,
	"math":  Math,
	"retry": Retry,
	"sys":   Sys,
	"text":  Text,
	"time":  Time,
})
