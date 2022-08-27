package defaults

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Sys = aggregateFunctionsToMap("sys", []types.Function{
	types.NewRawFunction("sys.now", []types.Argument{}, func([]any) (any, error) {
		now := time.Now().Unix()
		return now, nil
	}),
	types.MustNewFunction("sys.sleep", []types.Argument{
		{Name: "seconds"},
	}, func(seconds any) (any, error) {
		var duration time.Duration
		switch n := seconds.(type) {
		case int64:
			duration = time.Second * time.Duration(n)
		case float64:
			duration = time.Duration(float64(time.Second) * n)
		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("seconds is not a number: %v", seconds),
			}
		}

		time.Sleep(duration)
		return nil, nil
	}),
	types.MustNewFunction("sys.sleep_until", []types.Argument{
		{Name: "time"},
	}, func(seconds string) (any, error) {
		target, err := time.Parse(time.RFC3339Nano, seconds)
		if err != nil {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		target = target.Truncate(time.Microsecond)

		time.Sleep(time.Until(target))
		return nil, nil
	}),
	types.MustNewFunction("sys.get_env", []types.Argument{
		{Name: "name"},
		{Name: "default"},
	}, func(name, defaultValue string) (any, error) {
		value, ok := os.LookupEnv(name)
		if !ok {
			value = defaultValue
		}

		return value, nil
	}),
	types.MustNewFunction("sys.log", []types.Argument{
		{Name: "data", Default: types.SubstitutionNone},
		{Name: "severity", Default: "DEFAULT"},
		{Name: "text", Optional: true},
		{Name: "json", Optional: true},
	}, func(data any, severity string, text any, jsonValue map[string]any) (any, error) {
		if data != nil && text != nil || text != nil && jsonValue != nil || data != nil && jsonValue != nil {
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("one of data or text or json is needed, cannot specify multiple"),
			}
		}
		if data == types.SubstitutionNone && text == nil && jsonValue == nil {
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("one of data or text or json is required"),
			}
		}

		if data != types.SubstitutionNone {
			switch d := data.(type) {
			case map[string]any:
				jsonValue = d
			default:
				text = d
			}
		}

		if text != nil {
			b, err := json.Marshal(text)
			if err != nil {
				return nil, fmt.Errorf("json.Marshal: %w", err)
			}
			log.Printf(`{"severity":%q,"textPayload":%s}`, severity, string(b))
		} else {
			b, err := json.Marshal(jsonValue)
			if err != nil {
				return nil, fmt.Errorf("json.Marshal: %w", err)
			}
			log.Printf(`{"severity":%q,"jsonPayload":%s}`, severity, string(b))
		}
		return nil, nil
	}),
})
