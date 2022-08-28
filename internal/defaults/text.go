package defaults

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/samber/lo"
	"golang.org/x/text/encoding/ianaindex"
)

var Text = aggregateFunctionsToMap("text", []types.Function{
	types.MustNewFunction("text.decode", []types.Argument{
		{Name: "data"},
		{Name: "charset", Default: "UTF-8"},
	}, func(data []byte, charset string) (string, error) {
		enc, err := ianaindex.IANA.Encoding(charset)
		if err != nil {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}

		var s strings.Builder
		_, err = io.Copy(&s, enc.NewDecoder().Reader(bytes.NewReader(data)))
		if err != nil {
			return "", &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}

		return s.String(), nil
	}),
	types.MustNewFunction("text.encode", []types.Argument{
		{Name: "data"},
		{Name: "charset", Default: "UTF-8"},
	}, func(data, charset string) ([]byte, error) {
		enc, err := ianaindex.IANA.Encoding(charset)
		if err != nil {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}

		var b bytes.Buffer
		_, err = io.Copy(enc.NewEncoder().Writer(&b), strings.NewReader(data))
		if err != nil {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}

		return b.Bytes(), nil
	}),
	types.MustNewFunction("text.find_all", []types.Argument{
		{Name: "source"},
		{Name: "substr"},
	}, func(source, substr string) ([]any, error) {
		if !utf8.ValidString(source) {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("source is not valid utf8 string"),
			}
		}
		if !utf8.ValidString(substr) {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("substr is not valid utf8 string"),
			}
		}

		offset := 0
		indexes := []any{}
		for len(source[offset:]) != 0 {
			i := strings.Index(source, substr)
			if i == -1 {
				break
			}

			indexes = append(indexes, offset+i)
			offset += i + len(substr)
		}
		return indexes, nil
	}),
	types.MustNewFunction("text.find_all_regex", []types.Argument{
		{Name: "source"},
		{Name: "regexp"},
	}, func(source, regexpExpr string) ([]any, error) {
		if !utf8.ValidString(source) {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("source is not valid utf8 string"),
			}
		}
		if !utf8.ValidString(regexpExpr) {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("regexp is not valid utf8 string"),
			}
		}

		r, err := regexp.Compile(regexpExpr)
		if err != nil {
			return nil, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		matches := r.FindAllStringIndex(source, -1)
		indexes := lo.Map(matches, func(i []int, _ int) any { return i[0] })
		return indexes, nil
	}),
	types.MustNewFunction("text.match_regex", []types.Argument{
		{Name: "source"},
		{Name: "regexp"},
	}, func(source, regexpExpr string) (bool, error) {
		if !utf8.ValidString(source) {
			return false, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("source is not valid utf8 string"),
			}
		}
		if !utf8.ValidString(regexpExpr) {
			return false, &types.Error{
				Tag: types.ValueErrorTag,
				Err: fmt.Errorf("regexp is not valid utf8 string"),
			}
		}

		r, err := regexp.Compile(regexpExpr)
		if err != nil {
			return false, &types.Error{
				Tag: types.ValueErrorTag,
				Err: err,
			}
		}
		return r.MatchString(source), nil
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
