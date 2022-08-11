package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var List = map[string]any{
	"concat": types.MustNewFunction("list.concat", []types.Argument{
		{Name: "objs"},
		{Name: "val"},
	}, func(list []any, entry any) ([]any, error) {
		s := make([]any, len(list)+1)
		copy(s, list)
		s[len(s)-1] = entry
		return s, nil
	}),
}
