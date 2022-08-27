package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var List = aggregateFunctionsToMap("list", []types.Function{
	types.MustNewFunction("list.concat", []types.Argument{
		{Name: "objs"},
		{Name: "val"},
	}, func(list []any, entry any) ([]any, error) {
		return append(list[0:len(list):len(list)], entry), nil
	}),
})
