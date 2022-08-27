package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/workflow"
)

var basePathRegexp = regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/workflows/[^/]+/executions`)

type execution struct {
	mu sync.RWMutex

	Name               string    `json:"name"`
	StartTime          time.Time `json:"startTime"`
	EndTime            time.Time `json:"endTime,omitempty"`
	State              string    `json:"state"`
	Error              string    `json:"error,omitempty"`
	Argument           string    `json:"argument"`
	Result             string    `json:"result,omitempty"`
	WorkflowRevisionId string    `json:"workflowRevisionId"`
	CallLogLevel       string    `json:"callLogLevel"`
}

type httpHandler struct {
	workflowRoot atomic.Value
	idBase       uint64
	executions   sync.Map
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !basePathRegexp.MatchString(r.URL.Path) {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	if strings.HasSuffix(r.URL.Path, "/executions") {
		switch r.Method {
		case http.MethodGet:
			h.listExecutions(w, r)
			return

		case http.MethodPost:
			h.createExecution(w, r)
			return

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
	} else {
		executionID := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		if i := strings.LastIndexByte(executionID, ':'); i != -1 {
			customMethod := executionID[i+1:]
			executionID = executionID[:i]
			switch customMethod {
			case "cancel":
				if r.Method == http.MethodPost {
					h.cancelExecution(w, r, executionID)
					return
				}
				fallthrough

			default:
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
		}

		switch r.Method {
		case http.MethodGet:
			h.getExecution(w, r, executionID)
			return

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
	}
}

func (h *httpHandler) createExecution(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var ex *execution
	if err := json.NewDecoder(r.Body).Decode(&ex); err != nil {
		log.Printf("failed to decode request body: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	var args any
	if ex.Argument == "" {
		ex.Argument = "null"
	} else {
		if err := json.NewDecoder(strings.NewReader(ex.Argument)).Decode(&args); err != nil {
			log.Printf("failed to decode argument JSON: %v", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
	}

	// go go
	id := fmt.Sprintf("00000000-0000-0000-0000-%012x", atomic.AddUint64(&h.idBase, 1))
	ex.Name = r.URL.Path + "/" + id
	ex.StartTime = time.Now().UTC()
	ex.State = "ACTIVE"
	ex.WorkflowRevisionId = "000001-dummy"
	ex.CallLogLevel = "LOG_ALL_CALLS"
	h.executions.Store(id, ex)
	go h.execute(ex, args)
	resJSON(w, http.StatusOK, ex)
}

func (h *httpHandler) execute(ex *execution, args any) {
	ret, err := h.workflowRoot.Load().(workflow.WorkflowRoot).Execute(args)
	if err == nil {
		ex.mu.Lock()
		defer ex.mu.Unlock()
		ex.EndTime = time.Now().UTC()
		ex.State = "SUCCEEDED"
		var s strings.Builder
		if dumpErr := json.NewEncoder(&s).Encode(ret); dumpErr != nil {
			log.Printf("failed to encode workflow result: %v", dumpErr)
			log.Printf("result: %v", ret)
		} else {
			ex.Result = strings.TrimSuffix(s.String(), "\n")
		}
		return
	}

	ex.mu.Lock()
	defer ex.mu.Unlock()
	ex.EndTime = time.Now().UTC()
	ex.State = "FAILED"
	var exception types.Exception
	if errors.As(err, &exception) {
		var s strings.Builder
		if dumpErr := json.NewEncoder(&s).Encode(exception); dumpErr != nil {
			log.Printf("failed to encode workflow exception: %v", dumpErr)
			s.Reset()
			if dumpErr = json.NewEncoder(&s).Encode(err); dumpErr != nil {
				log.Printf("failed to encode workflow error: %v", dumpErr)
				ex.Error = fmt.Sprint(err)
			} else {
				ex.Error = strings.TrimSuffix(s.String(), "\n")
			}
		} else {
			ex.Error = strings.TrimSuffix(s.String(), "\n")
		}
	} else {
		log.Printf("failed to execute workflow: %v", err)
		var s strings.Builder
		if dumpErr := json.NewEncoder(&s).Encode(err); dumpErr != nil {
			log.Printf("failed to encode workflow error: %v", dumpErr)
			ex.Error = fmt.Sprint(err)
		} else {
			ex.Error = strings.TrimSuffix(s.String(), "\n")
		}
	}
}

func (h *httpHandler) listExecutions(w http.ResponseWriter, r *http.Request) {
	results := []*execution{}
	h.executions.Range(func(key, value any) bool {
		results = append(results, value.(*execution))
		return true
	})
	for _, ex := range results {
		ex.mu.RLock()
	}
	defer func() {
		for _, ex := range results {
			ex.mu.RUnlock()
		}
	}()
	sort.Slice(results, func(i, j int) bool {
		return results[i].StartTime.Before(results[j].StartTime)
	})

	resJSON(w, http.StatusOK, map[string][]*execution{"executions": results})
}

func (h *httpHandler) getExecution(w http.ResponseWriter, r *http.Request, id string) {
	ret, ok := h.executions.Load(id)
	if !ok {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	execution := ret.(*execution)

	execution.mu.RLock()
	defer execution.mu.RUnlock()
	resJSON(w, http.StatusOK, execution)
}

func (h *httpHandler) cancelExecution(w http.ResponseWriter, r *http.Request, id string) {
	http.Error(w, "Not Implemented", http.StatusNotImplemented) // patches welcome
}

func NewHTTPHandler(loader func() (workflow.WorkflowRoot, error)) (http.Handler, error) {
	root, err := loader()
	if err != nil {
		return nil, err
	}

	h := &httpHandler{}
	h.workflowRoot.Store(root)
	go func() {
		t := time.NewTicker(5 * time.Second)
		for range t.C {
			root, err := loader()
			if err != nil {
				log.Printf("failed to reload workflow: %v", err)
				continue
			}
			h.workflowRoot.Store(root)
		}
	}()
	return h, nil
}

func resJSON(w http.ResponseWriter, status int, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("json.MarshalIndent: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)+1))
	w.WriteHeader(status)

	if _, err = w.Write(b); err != nil {
		return fmt.Errorf("w.Write: %w", err)
	}
	if _, err = io.WriteString(w, "\n"); err != nil {
		return fmt.Errorf("io.WriteString: %w", err)
	}
	return nil
}
