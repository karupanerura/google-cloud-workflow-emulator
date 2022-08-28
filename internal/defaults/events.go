package defaults

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

const internalEventCallbackSymbol = "__INTERNAL_EVENT_CALLBACK"

type eventCallback struct {
	resultChan chan map[string]any
	received   atomic.Bool
	server     http.Server
}

var Events = aggregateFunctionsToMap("events", []types.Function{
	types.MustNewFunction("events.create_callback_endpoint", []types.Argument{
		{Name: "http_callback_method", Default: http.MethodPost},
	}, func(httpCallbackMethod string) (map[string]any, error) {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{
			IP:   net.IPv4zero,
			Port: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("net.Listen: %w", err)
		}

		callback := eventCallback{
			resultChan: make(chan map[string]any),
		}
		callback.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != httpCallbackMethod {
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
			if callback.received.Swap(true) {
				http.Error(w, "Callback request is already consumed", http.StatusBadRequest)
				return
			}

			headers := map[string]any{}
			for key := range r.Header {
				value := r.Header.Get(key)
				headers[key] = value
			}
			query := map[string]any{}
			if r.URL.RawQuery != "" {
				q := r.URL.Query()
				for key := range q {
					value := q.Get(key)
					query[key] = value
				}
			}

			var body any
			if ct := r.Header.Get("Content-Type"); ct != "" {
				mt, _, err := mime.ParseMediaType(ct)
				if err != nil {
					log.Println("Invalid Content-Type: ", err)
					http.Error(w, "Invalid Content-Type", http.StatusBadRequest)
					return
				}

				if mt == "application/json" || strings.HasPrefix(mt, "application/json+") || strings.HasSuffix(mt, "+json") {
					if err = json.NewDecoder(r.Body).Decode(&body); err != nil {
						log.Println("Invalid JSON format: ", err)
						http.Error(w, "Invalid JSON format", http.StatusBadRequest)
						return
					}
				} else {
					b, err := io.ReadAll(r.Body)
					if err != nil {
						log.Println("Failed to read request body: ", err)
						http.Error(w, "Failed to read request body:", http.StatusInternalServerError)
						return
					}
					body = string(b)
				}
			}
			w.WriteHeader(http.StatusNoContent)

			callback.resultChan <- map[string]any{
				"type": "HTTP",
				"http_request": map[string]any{
					"method":  r.Method,
					"headers": headers,
					"url":     r.URL.Path,
					"query":   query,
					"body":    body,
				},
				"received_time": time.Now().String(),
			}
		})
		go callback.server.Serve(listener)

		u := url.URL{
			Scheme: "http",
			Host:   listener.Addr().String(),
			Path:   "/",
		}
		log.Println("Created HTTP callback endpoint: ", u.String())

		return map[string]any{
			"url":                       u.String(),
			internalEventCallbackSymbol: &callback,
		}, nil
	}),
	types.MustNewFunction("events.await_callback", []types.Argument{
		{Name: "callback"},
		{Name: "timeout", Default: float64(43200.0)},
	}, func(m map[string]any, timeout float64) (any, error) {
		callback, ok := m[internalEventCallbackSymbol].(*eventCallback)
		if !ok {
			return nil, &types.Error{
				Tag: types.TypeErrorTag,
				Err: fmt.Errorf("callback is not a callback object"),
			}
		}

		defer func() {
			close(callback.resultChan)
			err := callback.server.Shutdown(context.Background())
			if err != nil {
				log.Printf("callback.server.Shutdown: %v", err)
			}
		}()

		t := time.NewTimer(time.Duration(timeout * float64(time.Second)))
		defer t.Stop()
		select {
		case <-t.C:
			return nil, &types.Error{
				Tag: types.TimeoutErrorTag,
			}
		case res := <-callback.resultChan:
			return res, nil
		}
	}),
})
