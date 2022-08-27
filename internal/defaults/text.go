package defaults

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

var Text = aggregateFunctionsToMap("text", []types.Function{
	types.MustNewFunction("text.replace_all_regex", []types.Argument{
		{Name: "source"},
		{Name: "substr"},
		{Name: "repl"},
	}, func(source, regexpExpr, repl string) (string, error) {
		if !utf8.ValidString(regexpExpr) {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("regexp is not valid utf8 string"),
			}
		}
		if !utf8.ValidString(repl) {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("repl is not valid utf8 string"),
			}
		}

		r, err := regexp.Compile(regexpExpr)
		if err != nil {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return r.ReplaceAllString(source, repl), nil
	}),
	types.MustNewFunction("text.replace_all", []types.Argument{
		{Name: "source"},
		{Name: "substr"},
		{Name: "repl"},
	}, func(source, substr, repl string) (string, error) {
		if !utf8.ValidString(substr) {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("substr is not valid utf8 string"),
			}
		}
		if !utf8.ValidString(repl) {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("repl is not valid utf8 string"),
			}
		}
		return strings.ReplaceAll(source, substr, repl), nil
	}),
	types.MustNewFunction("text.split", []types.Argument{
		{Name: "source"},
		{Name: "separator"},
	}, func(source, separator string) ([]any, error) {
		if len(separator) == 0 {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("separator is empty string"),
			}
		}
		if !utf8.ValidString(separator) {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("separator is not valid utf8 string"),
			}
		}

		result := []any{}
		for len(source) != 0 {
			i := strings.Index(source, separator)
			if i == -1 {
				result = append(result, source)
				return result, nil
			}

			result = append(result, source[:i])
			source = source[i+len(separator):]
		}
		return result, nil
	}),
	types.MustNewFunction("text.substring", []types.Argument{
		{Name: "source"},
		{Name: "start"},
		{Name: "end"},
	}, func(source string, start, end int64) (string, error) {
		if start < 0 {
			start = 0
		}
		if l := int64(len(source)); end > l {
			end = l
		}
		return source[start:end], nil
	}),
	types.MustNewFunction("text.to_lower", []types.Argument{
		{Name: "source"},
	}, func(source string) (string, error) {
		return strings.ToLower(source), nil
	}),
	types.MustNewFunction("text.to_upper", []types.Argument{
		{Name: "source"},
	}, func(source string) (string, error) {
		return strings.ToUpper(source), nil
	}),
	types.MustNewFunction("text.url_decode", []types.Argument{
		{Name: "source"},
	}, func(source string) (string, error) {
		decoded, err := url.QueryUnescape(source)
		if err != nil {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return decoded, nil
	}),
	types.MustNewFunction("text.url_encode", []types.Argument{
		{Name: "source"},
	}, func(source string) (string, error) {
		return strings.ReplaceAll(url.QueryEscape(source), "+", "%20"), nil
	}),
	types.MustNewFunction("text.url_encode_plus", []types.Argument{
		{Name: "source"},
	}, func(source string) (string, error) {
		return url.QueryEscape(source), nil
	}),
})
