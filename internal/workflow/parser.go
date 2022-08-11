package workflow

import (
	"bytes"
	"fmt"
	"io"

	"github.com/goccy/go-json"
	"github.com/goccy/go-yaml"
)

func ParseWorkflowYAML(r io.Reader) (WorkflowRoot, error) {
	yamlBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("io.ReadAll: %w", err)
	}

	jsonBytes, err := yaml.YAMLToJSON(yamlBytes)
	if err != nil {
		return nil, fmt.Errorf("yaml.YAMLToJSON: %w", err)
	}

	return ParseWorkflowJSON(bytes.NewReader(jsonBytes))
}

func ParseWorkflowJSON(r io.Reader) (WorkflowRoot, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()

	var root workflowRootDef
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("json.Decode: %w", err)
	}

	return root.compile()
}
