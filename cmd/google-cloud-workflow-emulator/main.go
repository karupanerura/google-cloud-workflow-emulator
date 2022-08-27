package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"
	"github.com/jessevdk/go-flags"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/server"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/workflow"
	"github.com/mattn/go-isatty"
)

type Option struct {
	File   string `short:"f" long:"file" description:"[REQUIRED] Workflow file" required:"true"`
	Args   string `long:"args" description:"[OPTIONAL] Workflow Arguments (JSON)" required:"false"`
	Listen string `short:"l" long:"listen" description:"[OPTIONAL] Listen host and port to emulate API" required:"false"`
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	var opt Option
	parser := flags.NewParser(&opt, flags.Default)
	_, err := parser.ParseArgs(args)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			return 0
		} else {
			parser.WriteHelp(os.Stdout)
			return 1
		}
	}
	if opt.Args != "" && opt.Listen != "" {
		parser.WriteHelp(os.Stdout)
		return 1
	}

	// server mode
	if opt.Listen != "" {
		err = serveWorkflow(opt.Listen, func() (workflow.WorkflowRoot, error) {
			return loadWorkflow(opt.File)
		})
		if err != nil {
			log.Printf("failed to serve workflow: %v", err)
			return 1
		}
		return 0
	}

	root, err := loadWorkflow(opt.File)
	if err != nil {
		log.Printf("failed to load workflow: %v", err)
		return 1
	}

	var workflowArgs any
	if opt.Args != "" {
		if err = json.Unmarshal([]byte(opt.Args), &workflowArgs); err != nil {
			log.Printf("failed to parse args as JSON: %v", err)
			return 1
		}
	}

	ret, err := root.Execute(workflowArgs)
	if err != nil {
		var exception types.Exception
		if errors.As(err, &exception) {
			if _, err = fmt.Fprintln(os.Stderr, exception.Error()); err != nil {
				log.Printf("failed to dump workflow error: %v", err)
			}
			if err = dumpJSON(os.Stderr, exception.Exception()); err != nil {
				log.Printf("failed to dump workflow error as JSON: %v", err)
			}
			return 1
		} else {
			log.Printf("failed to execute workflow: %v", err)
			return 1
		}
	}
	if ret != nil {
		if err = dumpJSON(os.Stdout, ret); err != nil {
			log.Printf("failed to dump workflow result: %v", err)
		}
	}

	return 0
}

func loadWorkflow(filePath string) (workflow.WorkflowRoot, error) {
	var parseWorkflow func(io.Reader) (workflow.WorkflowRoot, error)
	switch filepath.Ext(filePath) {
	case ".json":
		parseWorkflow = workflow.ParseWorkflowJSON
	case ".yaml":
		parseWorkflow = workflow.ParseWorkflowYAML
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", filePath)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%q): %w", filePath, err)
	}
	defer f.Close()

	root, err := parseWorkflow(f)
	if err != nil {
		return nil, fmt.Errorf("workflow.ParseWorkflow: %w", err)
	}
	return root, nil
}

func serveWorkflow(listen string, loader func() (workflow.WorkflowRoot, error)) error {
	handler, err := server.NewHTTPHandler(loader)
	if err != nil {
		return err
	}

	srv := http.Server{
		Handler: handler,
		Addr:    listen,
	}

	log.Printf("Listen HTTP on %s", listen)
	if err := srv.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func dumpJSON(w io.Writer, v any) error {
	opts := []json.EncodeOptionFunc{json.DisableHTMLEscape()}
	if f, ok := w.(interface{ Fd() uintptr }); ok {
		if isatty.IsTerminal(f.Fd()) {
			opts = append(opts, json.Colorize(json.DefaultColorScheme))
		}
	}

	b, err := json.MarshalIndentWithOption(v, "", "\t", opts...)
	if err != nil {
		return fmt.Errorf("json.MarshalIndentWithOption: %w", err)
	}

	if _, err = w.Write(b); err != nil {
		return fmt.Errorf("w.Write: %w", err)
	}
	if _, err = io.WriteString(w, "\n"); err != nil {
		return fmt.Errorf("io.WriteString: %w", err)
	}
	return nil
}
