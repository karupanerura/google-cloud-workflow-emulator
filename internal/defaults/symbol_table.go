package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var DefaultSymbolTable = types.SymbolTable{
	"base64": Base64,
	"events": nil,
	"experimental": map[string]any{
		"executions": nil,
	},
	"http":  HTTP,
	"json":  nil,
	"list":  List,
	"map":   nil,
	"math":  nil,
	"retry": nil,
	"sys":   Sys,
	"text":  nil,
	"time":  nil,
}
