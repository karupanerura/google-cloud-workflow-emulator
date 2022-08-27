package defaults

import "github.com/karupanerura/google-cloud-workflow-emulator/internal/types"

var ExperimentalExecutions = aggregateFunctionsToMap("experimental.executions", []types.Function{})
