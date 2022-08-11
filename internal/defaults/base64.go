package defaults

import (
	"encoding/base64"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Base64 = map[string]any{
	"encode": types.MustNewFunction("base64.encode", []types.Argument{
		{Name: "data"},
		{Name: "padding", Default: true},
	}, func(data string, padding bool) (string, error) {
		encoder := base64.RawStdEncoding
		if padding {
			encoder = base64.StdEncoding
		}
		return encoder.EncodeToString([]byte(data)), nil
	}),
	"decode": types.MustNewFunction("base64.decode", []types.Argument{
		{Name: "data"},
		{Name: "padding", Default: true},
	}, func(data string, padding bool) (string, error) {
		decoder := base64.RawStdEncoding
		if padding {
			decoder = base64.StdEncoding
		}

		b, err := decoder.DecodeString(data)
		if err != nil {
			return "", &types.Error{
				Tag:   types.ValueErrorTag,
				Err:   err,
				Extra: nil,
			}
		}

		return string(b), nil
	}),
}
