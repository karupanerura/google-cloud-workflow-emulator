package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var Events = aggregateFunctionsToMap("events", []types.Function{})
