package defaults

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/mitchellh/mapstructure"
)

var JSON = aggregateFunctionsToMap("json", []types.Function{
	types.MustNewFunction("json.decode", []types.Argument{
		{Name: "data"},
	}, func(dataAny any) (ret any, err error) {
		var data []byte
		switch v := dataAny.(type) {
		case []byte:
			data = v
		case string:
			data = []byte(v)
		default:
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("data is not a string or bytes"),
			}
		}

		err = json.Unmarshal(data, &ret)
		if err != nil {
			err = &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return
	}),
	types.MustNewFunction("json.encode", []types.Argument{
		{Name: "data"},
		{Name: "indent", Default: false},
	}, encodeJSON),
	types.MustNewFunction("json.encode_to_string", []types.Argument{
		{Name: "data"},
		{Name: "indent", Default: false},
	}, func(data, indent any) (string, error) {
		ret, err := encodeJSON(data, indent)
		if err != nil {
			return "", err
		}
		return string(ret), nil
	}),
})

func encodeJSON(data, indent any) ([]byte, error) {
	type indentConfig struct {
		Prefix string `mapstructure:"prefix"`
		Indent string `mapstructure:"indent"`
	}

	var config *indentConfig
	switch v := indent.(type) {
	case bool:
		if v {
			config = &indentConfig{Indent: "    "}
		}
	case map[string]any:
		err := mapstructure.Decode(v, &config)
		if err != nil {
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: err,
			}
		}
		if _, ok := v["indent"]; !ok {
			config.Indent = "    "
		}
	}

	if config != nil {
		ret, err := json.MarshalIndent(data, config.Prefix, config.Indent)
		if err != nil {
			err = &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return ret, nil
	} else {
		ret, err := json.Marshal(data)
		if err != nil {
			err = &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return ret, nil
	}
}
