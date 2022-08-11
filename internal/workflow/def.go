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

	anonStep, err := d.stepDef.compile(d.name, defaultNextStepName)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", d.name, err)
	}

	step, ok := anonStep.(Step)
	if !ok || step.Name() != d.name {
		return nil, fmt.Errorf("%s: invalid step structure", d.name)
	}

	return step, nil
}

type anonymousStepDef map[string]json.RawMessage

var firstLevelFieldsOfStep = []string{"call", "args", "try", "retry", "except", "assign", "steps", "raise", "switch", "result", "next", "return"}

func (def anonymousStepDef) compile(stepName, defaultNextStepName StepName) (AnonymousStep, error) {
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
	if found["next"] && found["return"] && found["raise"] {
		return nil, fmt.Errorf("conflict next and return and raise")
	}

	if found["call"] && found["args"] {
		return newCallStep(stepName, def, defaultNextStepName)
	} else if found["switch"] {
		return newSwitchStep(stepName, def, defaultNextStepName)
	} else if found["assign"] {
		return newAssignStep(stepName, def, defaultNextStepName)
	} else if found["try"] && (found["retry"] || found["except"]) {
		return newTryStep(stepName, def, defaultNextStepName)
	} else if found["for"] {
		panic("TODO")
	} else if found["parallel"] {
		panic("TODO")
	} else if found["steps"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newStepsStep(stepName, def, defaultNextStepName)
	} else if found["raise"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newRaiseStep(stepName, def)
	} else if found["return"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newReturnStep(stepName, def)
	} else if found["next"] {
		if len(found) != 1 {
			panic("should not reach at here")
		}
		return newNextStep(stepName, def)
	}

	return nil, fmt.Errorf("invalid step")
}
