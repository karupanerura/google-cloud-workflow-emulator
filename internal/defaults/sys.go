package defaults

import (
	"fmt"
	"log"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Sys = map[string]any{
	"log": types.MustNewFunction("sys.log", []types.Argument{
		{Name: "data", Optional: true},
		{Name: "severity", Default: "DEFAULT"},
		{Name: "text", Optional: true},
		{Name: "json", Optional: true},
	}, func(data any, severity string, text any, jsonValue map[string]any) (any, error) {
		if data != nil && text != nil || text != nil && jsonValue != nil || data != nil && jsonValue != nil {
			return nil, fmt.Errorf("one of data or text or json is needed, cannot specify multiple")
		}
		if data == nil && text == nil && jsonValue == nil {
			return nil, fmt.Errorf("one of data or text or json is required")
		}

		if data != nil {
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
}
