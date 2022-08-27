package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var Retry = mergeMaps(
	map[string]any{
		"default_backoff": map[string]any{
			"initial_delay": float64(1),
			"max_delay":     float64(60),
			"multiplier":    float64(2),
		},
	},
	aggregateFunctionsToMap("retry", []types.Function{
		types.NewRawFunction("retry.always", []types.Argument{
			{Name: "exception"},
		}, func([]any) (any, error) {
			return true, nil
		}),
		types.NewRawFunction("retry.never", []types.Argument{
			{Name: "exception"},
		}, func([]any) (any, error) {
			return false, nil
		}),
	}),
)
