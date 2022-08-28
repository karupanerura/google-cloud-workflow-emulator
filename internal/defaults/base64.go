package defaults

import (
	"encoding/base64"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Base64 = aggregateFunctionsToMap("base64", []types.Function{
	types.MustNewFunction("base64.encode", []types.Argument{
		{Name: "data"},
		{Name: "padding", Default: true},
	}, func(data []byte, padding bool) (string, error) {
		encoder := base64.RawStdEncoding
		if padding {
			encoder = base64.StdEncoding
		}
		return encoder.EncodeToString(data), nil
	}),
	types.MustNewFunction("base64.decode", []types.Argument{
		{Name: "data"},
		{Name: "padding", Default: true},
	}, func(data string, padding bool) ([]byte, error) {
		decoder := base64.RawStdEncoding
		if padding {
			decoder = base64.StdEncoding
		}

		b, err := decoder.DecodeString(data)
		if err != nil {
			return nil, &types.Error{
				Tag:   types.ValueErrorTag,
				Err:   err,
				Extra: nil,
			}
		}

		return b, nil
	}),
})
