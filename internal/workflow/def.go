package workflow

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type workflowRootDef map[string]workflowDef

func (d workflowRootDef) compile() (WorkflowRoot, error) {
	root := make(WorkflowRoot, len(d))
	for name, def := range d {
		wf, err := def.compile(name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		root[name] = wf
	}
	if _, ok := root["main"]; !ok {
		return nil, fmt.Errorf("main is required in workflow")
	}
	if len(root["main"].Params) > 1 {
		return nil, fmt.Errorf("main can have a single params only, multiple params are not supported")
	}

	return root, nil
}

type workflowDef struct {
	Params []any              `json:"params"`
	Steps  []*workflowStepDef `json:"steps"`
}

func (d *workflowDef) compile(name string) (*Workflow, error) {
	if len(d.Steps) == 0 {
		return nil, fmt.Errorf("empty steps")
	}

	wf := Workflow{
		Name:    name,
		Params:  make([]types.Argument, len(d.Params)),
		stepMap: map[StepName]Step{},
	}

	// parse params
	for i, param := range d.Params {
		switch v := param.(type) {
		case map[string]any:
			if len(v) != 1 {
				return nil, fmt.Errorf("params[%d]: invalid type", i)
			}
			for key, value := range v {
				v, err := decodeJSONNumberRecursive(value)
				if err != nil {
					return nil, fmt.Errorf("params[%d]: invalid number", i)
				}

				wf.Params[i] = types.Argument{
					Name:    key,
					Default: v,
				}
			}

		case string:
			wf.Params[i] = types.Argument{
				Name: v,
			}

		default:
			return nil, fmt.Errorf("params[%d]: invalid type", i)
		}
	}

	// parse steps
	for i, stepDef := range d.Steps {
		if _, duplicated := wf.stepMap[stepDef.name]; duplicated {
			return nil, fmt.Errorf("%s: duplicated step name in steps", stepDef.name)
		}

		var defaultNextStepName StepName
		if i == len(d.Steps)-1 {
			defaultNextStepName = "end"
		} else {
			defaultNextStepName = d.Steps[i+1].name
		}

		var err error
		wf.stepMap[stepDef.name], err = stepDef.compile(defaultNextStepName)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", stepDef.name, err)
		}

		if wf.entryStep == nil {
			wf.entryStep = wf.stepMap[stepDef.name]
		}
	}

	return &wf, nil
}

type workflowStepDef struct {
	name    StepName         `json:"-"`
	stepDef anonymousStepDef `json:"-"`
}

var _ json.Unmarshaler = (*workflowStepDef)(nil)

func (d *workflowStepDef) UnmarshalJSON(b []byte) error {
	var m map[StepName]anonymousStepDef
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("unexpected workflow step structure: %w", err)
	}
	if len(m) != 1 {
		return fmt.Errorf("invalid workflow step structure: %s", string(b))
	}

	for d.name = range m {
		// XXX: assign map key as step name to d.name
	}
	d.stepDef = m[d.name]
	return nil
}

func (d *workflowStepDef) compile(defaultNextStepName StepName) (Step, error) {
	if d.name == "end" {
		return nil, fmt.Errorf("cannot use the special step name %q", d.name)
	}

	nextStep := defaultNextStepName
	anonStep, err := d.stepDef.compile()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", d.name, err)
	}

	return &namedStep{
		name: d.name,
		step: anonStep,
		next: nextStep,
	}, nil
}

type anonymousStepDef map[string]json.RawMessage

var firstLevelFieldsOfStep = []string{"call", "args", "try", "retry", "except", "for", "parallel", "assign", "steps", "raise", "switch", "result", "next", "return"}

func (def anonymousStepDef) compile() (AnonymousStep, error) {
	if nextJSON, ok := def["next"]; ok {
		var next StepName
		if err := json.Unmarshal(nextJSON, &next); err != nil {
			return nil, fmt.Errorf("invalid next: %w", err)
		}
		delete(def, "next")
		if len(def) == 0 {
			return &nextStep{
				step: nopStep{},
				next: next,
			}, nil
		}

		step, err := def.compile()
		if err != nil {
			return nil, err
		}
		return &nextStep{
			step: step,
			next: next,
		}, nil
	}

	found := map[string]bool{}
	for _, name := range firstLevelFieldsOfStep {
		if _, ok := def[name]; ok {
			found[name] = true
		}
	}

	if len(found) == 0 {
		return nil, fmt.Errorf("no effective fields in the step")
	}
	if len(found) != len(def) {
		return nil, fmt.Errorf("%d not effective fields in the step", len(def)-len(found))
	}
	if found["return"] && found["raise"] {
		return nil, fmt.Errorf("conflict return and raise")
	}

	if found["call"] {
		return newCallStep(def)
	} else if found["switch"] {
		return newSwitchStep(def)
	} else if found["assign"] {
		return newAssignStep(def)
	} else if found["try"] && (found["retry"] || found["except"]) {
		return newTryStep(def)
	} else if found["for"] {
		return newForStep(def, nil)
	} else if found["parallel"] {
		return newParallelStep(def)
	} else if found["steps"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newAnonymousStepsStep(def)
	} else if found["raise"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newRaiseStep(def)
	} else if found["return"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newReturnStep(def)
	}

	return nil, fmt.Errorf("invalid step")
}
