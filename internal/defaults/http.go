package defaults

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"net/url"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
)

type bodyKind int

const (
	noBody bodyKind = iota
	jsonBody
	stringBody
	queryFormBody
)

var sharedHTTPClient = httpClient{
	defaultBodyKind:        jsonBody,
	oidcTokenSourceCache:   map[string]oauth2.TokenSource{},
	oauth2TokenSourceCache: map[string]oauth2.TokenSource{},
}

var HTTP = mergeMaps(
	aggregateFunctionsToMap("http", []types.Function{
		types.MustNewFunction("http.request", []types.Argument{
			{Name: "method"},
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "body", Optional: true},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(method, rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(method, rawURL, timeout, rawBody, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.get", []types.Argument{
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(rawURL string, timeout float64, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(http.MethodGet, rawURL, timeout, nil, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.post", []types.Argument{
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "body", Optional: true},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(http.MethodPost, rawURL, timeout, rawBody, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.put", []types.Argument{
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "body", Optional: true},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(http.MethodPut, rawURL, timeout, rawBody, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.patch", []types.Argument{
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "body", Optional: true},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(http.MethodPatch, rawURL, timeout, rawBody, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.delete", []types.Argument{
			{Name: "url"},
			{Name: "timeout", Default: float64(300)},
			{Name: "body", Optional: true},
			{Name: "headers", Optional: true},
			{Name: "query", Optional: true},
			{Name: "auth", Optional: true},
		}, func(rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
			return sharedHTTPClient.request(http.MethodDelete, rawURL, timeout, rawBody, rawHeaders, rawQuery, auth)
		}),
		types.MustNewFunction("http.default_retry_predicate", []types.Argument{
			{Name: "exception"},
		}, func(exception map[string]any) (bool, error) {
			codeAny, ok := exception["code"]
			if !ok {
				return false, nil
			}

			code, ok := codeAny.(int64)
			if !ok {
				return false, nil
			}

			switch code {
			case 429, 502, 503, 504:
				return true, nil
			default:
				return false, nil
			}
		}),
		types.MustNewFunction("http.default_retry_predicate_non_idempotent", []types.Argument{
			{Name: "exception"},
		}, func(exception map[string]any) (bool, error) {
			codeAny, ok := exception["code"]
			if !ok {
				return false, nil
			}

			code, ok := codeAny.(int64)
			if !ok {
				return false, nil
			}

			switch code {
			case 429, 503:
				return true, nil
			default:
				return false, nil
			}
		}),
	}),
	map[string]any{
		"default_retry": map[string]any{
			"predicate":   "${http.default_retry_predicate}",
			"max_retries": int64(5),
			"backoff":     Retry["default_backoff"],
		},
		"default_retry_non_idempotent": map[string]any{
			"predicate":   "${http.default_retry_predicate_non_idempotent}",
			"max_retries": int64(5),
			"backoff":     Retry["default_backoff"],
		},
	},
)

type httpClient struct {
	defaultBodyKind        bodyKind
	oidcTokenSourceCache   map[string]oauth2.TokenSource
	oauth2TokenSourceCache map[string]oauth2.TokenSource
}

func (c *httpClient) request(method, rawURL string, timeout float64, rawBody any, rawHeaders, rawQuery, auth map[string]any) (map[string]any, error) {
	var bodyFormat bodyKind
	var reqBody io.Reader
	switch method {
	case http.MethodDelete:
		if rawBody == nil {
			break
		}
		fallthrough

	case http.MethodPost, http.MethodPut, http.MethodPatch:
		var err error
		bodyFormat, err = c.detectBodyFormat(rawHeaders)
		if err != nil {
			return nil, err
		}

		reqBody, err = c.createBodyReader(bodyFormat, rawBody)
		if err != nil {
			return nil, err
		}

	default:
		// nothing to do
	}

	u, err := c.createURL(rawURL, rawQuery)
	if err != nil {
		return nil, err
	}

	log.Println(method, u.String())
	req, err := http.NewRequest(method, u.String(), reqBody)
	if err != nil {
		return nil, fmt.Errorf("http.NewRequestWithContext: %w", err)
	}

	err = c.setRequestHeaders(req.Header, rawHeaders, bodyFormat)
	if err != nil {
		return nil, err
	}
	err = c.setAuthHeaders(u, req, auth)
	if err != nil {
		return nil, err
	}

	if timeout != 0 {
		ctx, cancel := context.WithTimeout(req.Context(), time.Duration(math.Floor(timeout*float64(time.Second))))
		defer cancel()
		req = req.WithContext(ctx)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.DefaultClient.Do: %w", err)
	}
	defer res.Body.Close()

	isJSON := false
	if ct := res.Header.Get("Content-Type"); ct != "" {
		mediaType, _, err := mime.ParseMediaType(ct)
		if err == nil {
			isJSON = mediaType == "application/json"
		}
	}

	var resBody any
	if isJSON {
		err = json.NewDecoder(res.Body).Decode(&resBody)
		if err != nil {
			return nil, fmt.Errorf("json.Decode: %w", err)
		}
	} else {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, fmt.Errorf("io.ReadAll: %w", err)
		}
		resBody = b
	}

	resHeaders := map[string]any{}
	for name := range res.Header {
		resHeaders[name] = res.Header.Get(name)
	}

	return map[string]any{
		"code":    res.StatusCode,
		"headers": resHeaders,
		"body":    resBody,
	}, nil
}

func (c *httpClient) detectBodyFormat(rawHeaders map[string]any) (bodyKind, error) {
	for name := range rawHeaders {
		if !strings.EqualFold(name, "Content-Type") {
			continue
		}

		value, ok := rawHeaders[name].(string)
		if !ok {
			return 0, fmt.Errorf("unsupported type for rawQuery value for name=%s: %T", name, value)
		}

		mediaType, _, err := mime.ParseMediaType(value)
		if err != nil {
			return 0, fmt.Errorf("invalid Content-Type %q: %w", value, err)
		}

		if strings.HasSuffix(mediaType, "text/") {
			return stringBody, nil
		} else if mediaType == "application/x-www-form-urlencoded" {
			return queryFormBody, nil
		} else if mediaType == "application/json" {
			return jsonBody, nil
		} else if strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json") {
			return jsonBody, nil
		} else {
			return 0, fmt.Errorf("unsupported Content-Type: %q", value)
		}
	}

	return c.defaultBodyKind, nil
}

func (c *httpClient) createBodyReader(bodyFormat bodyKind, rawBody any) (io.Reader, error) {
	switch body := rawBody.(type) {
	case string:
		switch bodyFormat {
		case queryFormBody:
			if _, err := url.ParseQuery(body); err != nil {
				return nil, fmt.Errorf("url.ParseQuery: %w", err)
			}
			fallthrough
		case stringBody:
			return strings.NewReader(body), nil
		default:
			return nil, fmt.Errorf("invalid body type with content-type: %T", rawBody)
		}

	case map[string]any:
		switch bodyFormat {
		case jsonBody:
			b, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("json.Marshal: %w", err)
			}

			return bytes.NewReader(b), nil
		default:
			return nil, fmt.Errorf("invalid body type with content-type: %T", rawBody)
		}

	default:
		return nil, fmt.Errorf("invalid body type with content-type: %T", rawBody)
	}
}

func (c *httpClient) createURL(rawURL string, rawQuery map[string]any) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("url.Parse: %w", err)
	}

	if rawQuery != nil {
		query := u.Query()
		for name, value := range rawQuery {
			switch v := value.(type) {
			case string:
				query.Set(name, v)
			case int64:
				query.Set(name, strconv.FormatInt(v, 10))
			case float64:
				query.Set(name, strconv.FormatFloat(v, 'f', -1, 64))
			default:
				return nil, fmt.Errorf("unsupported type for query value for name=%s: %T", name, v)
			}
		}
		u.RawQuery = query.Encode()
	}

	return u, nil
}

func (c *httpClient) setRequestHeaders(header http.Header, rawHeaders map[string]any, bodyFormat bodyKind) error {
	for field, value := range rawHeaders {
		switch v := value.(type) {
		case string:
			header.Set(field, v)
		case int64:
			header.Set(field, strconv.FormatInt(v, 10))
		case float64:
			header.Set(field, strconv.FormatFloat(v, 'f', -1, 64))
		default:
			return fmt.Errorf("unsupported type for header value for field=%s: %T", field, v)
		}
	}
	if _, ok := header[http.CanonicalHeaderKey("Content-Type")]; !ok {
		switch bodyFormat {
		case jsonBody:
			header.Set("Content-Type", "application/json")
		case stringBody:
			header.Set("Content-Type", "text/plain")
		case queryFormBody:
			header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	return nil
}

func (c *httpClient) setAuthHeaders(u *url.URL, req *http.Request, auth map[string]any) error {
	if auth == nil {
		return nil
	}

	typ, ok := auth["type"].(string)
	if !ok {
		return fmt.Errorf("auth.type is required")
	}
	switch typ {
	case "OIDC":
		return c.setOIDCAuthHeaders(u, req, auth)

	case "OAuth2":
		return c.setOAuth2Headers(req, auth)

	default:
		return fmt.Errorf("unknown auth.type: %s", typ)
	}
}

func (c *httpClient) setOIDCAuthHeaders(u *url.URL, req *http.Request, auth map[string]any) error {
	audience, ok := auth["audience"].(string)
	if !ok {
		audience = u.String()
	}

	ts, ok := c.oidcTokenSourceCache[audience]
	if !ok {
		// XXX: dirty hack for authorized_user default application credential
		creds, err := google.FindDefaultCredentials(context.Background())
		if err == nil {
			if isAuthorizedUser(creds.JSON) == nil {
				ts = &gcloudAuthPrintIdentityTokenSource{}
				c.oidcTokenSourceCache[audience] = ts
				ok = true
			}
		}

		if !ok {
			ts, err = idtoken.NewTokenSource(context.Background(), audience)
			if err != nil {
				return fmt.Errorf("idtoken.NewTokenSource: %w", err)
			}
			c.oidcTokenSourceCache[audience] = ts
		}
	}

	token, err := ts.Token()
	if err != nil {
		return fmt.Errorf("ts.Token: %w", err)
	}

	token.SetAuthHeader(req)
	return nil
}

var oauth2ScopeSeparatorSet = map[byte]struct{}{
	' ': {},
	',': {},
}

func (c *httpClient) setOAuth2Headers(req *http.Request, auth map[string]any) error {
	var scopes []string
	for _, key := range []string{"scope", "scopes"} {
		v, ok := auth[key]
		if !ok {
			continue
		}
		if scopes != nil {
			return fmt.Errorf("cannot set scope and scopes both")
		}

		switch vv := v.(type) {
		case string:
			i := 0
			for j := 0; j < len(vv); j++ {
				c := vv[j]
				if _, ok := oauth2ScopeSeparatorSet[c]; !ok {
					continue
				}

				scopes = append(scopes, vv[i:j])
				i = j + 1
			}

		case []string:
			scopes = vv

		case []any:
			for i, vvv := range vv {
				if s, ok := vvv.(string); ok {
					scopes = append(scopes, s)
				} else {
					return fmt.Errorf("invalid auth.%s[%d] type: %T", key, i, v)
				}
			}

		default:
			return fmt.Errorf("invalid auth.%s type: %T", key, v)
		}
	}

	sort.Strings(scopes)
	key := strings.Join(scopes, "::")
	ts, ok := c.oauth2TokenSourceCache[key]
	if !ok {
		creds, err := transport.Creds(context.Background(), option.WithScopes(scopes...))
		if err != nil {
			return fmt.Errorf("transport.Creds: %w", err)
		}

		ts = creds.TokenSource
	}

	token, err := ts.Token()
	if err != nil {
		return fmt.Errorf("ts.Token: %w", err)
	}

	token.SetAuthHeader(req)
	return nil
}

type gcloudAuthPrintIdentityTokenSource struct {
	buf   strings.Builder
	token oauth2.Token
}

func (ts *gcloudAuthPrintIdentityTokenSource) Token() (*oauth2.Token, error) {
	if ts.token.Expiry.Before(time.Now()) {
		return &ts.token, nil
	}

	defer ts.buf.Reset()
	cmd := exec.Command("gcloud", "auth", "print-identity-token")
	cmd.Stderr = io.Discard
	cmd.Stdout = &ts.buf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gcloud auth print-identity-token: %w", err)
	}

	ts.token.TokenType = "Bearer"
	ts.token.AccessToken = ts.buf.String()

	parts := strings.SplitN(ts.token.AccessToken, ".", 3)
	if rawJSON, err := base64.RawStdEncoding.DecodeString(parts[1]); err != nil {
		return nil, fmt.Errorf("gcloud auth print-identity-token JWT: %w", err)
	} else {
		var v struct {
			Exp int64 `json:"exp"`
		}
		if err = json.Unmarshal(rawJSON, &v); err != nil {
			return nil, fmt.Errorf("gcloud auth print-identity-token JWT: %w", err)
		}
		ts.token.Expiry = time.Unix(v.Exp, 0)
	}

	return &ts.token, nil
}

func isAuthorizedUser(data []byte) error {
	var v struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if v.Type != "authorized_user" {
		return fmt.Errorf("not authorized_user: %q", v.Type)
	}
	return nil
}
