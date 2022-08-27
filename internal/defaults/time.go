package defaults

import (
	"fmt"
	"math"
	"time"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Time = aggregateFunctionsToMap("time", []types.Function{
	types.MustNewFunction("time.format", []types.Argument{
		{Name: "seconds"},
		{Name: "timezone", Optional: true},
	}, func(seconds any, timeZone string) (string, error) {
		var t time.Time
		switch v := seconds.(type) {
		case int64:
			t = time.Unix(v, 0)
		case float64:
			t = time.UnixMicro(int64(math.Floor(v * 1000 * 1000)))
		default:
			return "", &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("seconds is not a number: %v", seconds),
			}
		}

		return t.Format(time.RFC3339Nano), nil
	}),
	types.MustNewFunction("time.parse", []types.Argument{
		{Name: "value"},
	}, func(value string) (int64, error) {
		t, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return 0, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}

		return t.Unix(), nil
	}),
})
