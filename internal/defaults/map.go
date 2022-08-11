package defaults

import (
	"fmt"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Map = map[string]any{
	"get": types.MustNewFunction("map.get", []types.Argument{
		{Name: "map", Optional: true},
		{Name: "keys"},
	}, func(m map[string]any, keysRaw any) (any, error) {
		if m == nil {
			return nil, nil
		}

		var keys []string
		switch v := keysRaw.(type) {
		case string:
			keys = []string{v}
		case []any:
			keys = make([]string, len(v))
			for i, vv := range v {
				vv, ok := vv.(string)
				if !ok {
					return nil, &types.Error{
						Tag: types.TypeErrorTag,
						Err: fmt.Errorf("keys must be string or string array"),
					}
				}

				keys[i] = vv
			}
		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("keys must be string or string array"),
			}
		}

		var context any = m
		for _, key := range keys {
			m, ok := context.(map[string]any)
			if !ok {
				return nil, nil
			}

			context = m[key]
		}
		return context, nil
	}),
}
