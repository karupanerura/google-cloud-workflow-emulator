package workflow

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/goccy/go-json"
	"github.com/k0kubun/pp"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/defaults"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/expression"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
)

type WorkflowRoot map[string]*Workflow

func (r WorkflowRoot) Execute(args any) (any, error) {
	log.Printf("[DEBUG] execute from root")

	mainWorflow, ok := r["main"]
	if !ok {
		return nil, fmt.Errorf("main workflow is not defined")
	}

	argsSymbolTable := types.SymbolTable{}
	if len(mainWorflow.Params) == 1 {
		argsSymbolTable[mainWorflow.Params[0].Name] = args
	}

	st := types.SymbolTable{}
	for name, workflow := range r {
		if name == "main" {
			continue
		}

		name := name
		workflow := workflow
		st[name] = types.NewRawFunction(name, workflow.Params, func(args []any) (any, error) {
			log.Printf("[DEBUG] workflow %s", name)
			st := st.Inherit(defaults.DefaultSymbolTable)
			for i, param := range workflow.Params {
				st[param.Name] = args[i]
			}
			return workflow.Execute(st)
		})
	}

	return mainWorflow.Execute(argsSymbolTable.Inherit(st.Inherit(defaults.DefaultSymbolTable)))
}

type Workflow struct {
	Name   string
	Params []types.Argument

	entryStep Step
	stepMap   map[StepName]Step
}

func (w *Workflow) Execute(symbolTable types.SymbolTable) (ret any, err error) {
	log.Printf("[DEBUG] workflow %s", w.Name)
	for _, param := range w.Params {
		if _, ok := symbolTable[param.Name]; ok {
			continue
		}
		if param.Optional {
			continue
		}
		if param.Default != nil {
			symbolTable[param.Name] = param.Default
		}
		return nil, fmt.Errorf("missing param: %s", param.Name)
	}

	ev := expression.Evaluator{SymbolTable: symbolTable.Inherit(defaults.DefaultSymbolTable)}
	step := w.entryStep
	for step != nil {
		log.Printf("[DEBUG] step %s", step.Name())

		var nextStepName StepName
		ret, nextStepName, err = step.Execute(&ev)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", step.Name(), err)
		}
		if nextStepName == "end" {
			return ret, nil
		} else if nextStepName == "" {
			return nil, fmt.Errorf("%s: next step is not defined", step.Name())
		}

		nextStep, ok := w.stepMap[nextStepName]
		if !ok {
			return nil, fmt.Errorf("%s: not found", nextStepName)
		}

		step = nextStep
	}
	return
}

type StepName string

type AnonymousStep interface {
	Execute(*expression.Evaluator) (any, StepName, error)
}

type Step interface {
	Name() StepName
	AnonymousStep
}

type assignStep struct {
	name         StepName
	assigns      []assignOperation
	nextStepName StepName
}

type assignOperation struct {
	left  *expression.Expr
	right any
}

func newAssignStep(stepName StepName, def anonymousStepDef, defaultNextStepName StepName) (*assignStep, error) {
	var assignDef []map[string]any
	err := unmarshalJSONUseNumber(def["assign"], &assignDef)
	if err != nil {
		return nil, fmt.Errorf("invalid assign: %w", err)
	}

	assigns := make([]assignOperation, len(assignDef))
	for i, def := range assignDef {
		if len(def) != 1 {
			return nil, fmt.Errorf("invalid assign[%d]: too many defs", i)
		}
		for key, value := range def {
			value, err = decodeJSONNumberRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("invalid any numbers of assign[%d]: %w", i, err)
			}

			assigns[i].left, err = expression.ParseExpr(key)
			if err != nil {
				return nil, fmt.Errorf("invalid assign[%d]: %w", i, err)
			}
			if !assigns[i].left.CanReference() {
				return nil, fmt.Errorf("invalid assign[%d]: cannot be assign to %q", i, key)
			}

			assigns[i].right, err = expression.ExpandExprRecursive(value)
			if err != nil {
				return nil, fmt.Errorf("invalid assign[%d]: %w", i, err)
			}
		}
	}

	return &assignStep{
		name:         stepName,
		assigns:      assigns,
		nextStepName: defaultNextStepName,
	}, nil
}

func (s *assignStep) Name() StepName {
	return s.name
}

func (s *assignStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	for i, assign := range s.assigns {
		ref, err := ev.ResolveReference(assign.left)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}

		variable, err := ref.ResolveVariable(ev.SymbolTable)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}

		value, err := ev.EvaluateValueRecursive(assign.right)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}

		variable.Set(value)
	}
	return nil, s.nextStepName, nil
}

type returnStep struct {
	name        StepName
	returnValue any
}

func newReturnStep(stepName StepName, def anonymousStepDef) (*returnStep, error) {
	var returnDef any
	err := json.Unmarshal(def["return"], &returnDef)
	if err != nil {
		return nil, fmt.Errorf("invalid return: %w", err)
	}

	returnValue, err := expression.ExpandExprRecursive(returnDef)
	if err != nil {
		return nil, fmt.Errorf("invalid return: %w", err)
	}

	return &returnStep{
		name:        stepName,
		returnValue: returnValue,
	}, nil
}

func (s *returnStep) Name() StepName {
	return s.name
}

func (s *returnStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	ret, err := ev.EvaluateValueRecursive(s.returnValue)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", s.returnValue, err)
	}

	return ret, "end", nil
}

type nextStep struct {
	name StepName
	next StepName
}

func newNextStep(stepName StepName, def anonymousStepDef) (*nextStep, error) {
	var next StepName
	err := json.Unmarshal(def["next"], &next)
	if err != nil {
		return nil, fmt.Errorf("invalid next: %w", err)
	}

	return &nextStep{
		name: stepName,
		next: next,
	}, nil
}

func (s *nextStep) Name() StepName {
	return s.name
}

func (s *nextStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	return nil, s.next, nil
}

type raiseStep struct {
	name       StepName
	raiseValue any
}

func newRaiseStep(stepName StepName, def anonymousStepDef) (*raiseStep, error) {
	var raiseValue any
	err := json.Unmarshal(def["raise"], &raiseValue)
	if err != nil {
		return nil, fmt.Errorf("invalid raise: %w", err)
	}

	switch v := raiseValue.(type) {
	case map[string]any:
		// OK

	case string:
		raiseValue, err = expression.ExpandExpr(v)
		if err != nil {
			return nil, fmt.Errorf("invalid raise: %w", err)
		}

	default:
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("invalid raise: must be string or map"),
		}
	}

	return &raiseStep{
		name:       stepName,
		raiseValue: raiseValue,
	}, nil
}

func (s *raiseStep) Name() StepName {
	return s.name
}

func (s *raiseStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	if expr, ok := s.raiseValue.(*expression.Expr); ok {
		ret, err := ev.EvaluateValue(expr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid raise: %w", err)
		}

		return s.raise(ev, ret)
	}

	return s.raise(ev, s.raiseValue)
}

func (s *raiseStep) raise(ev *expression.Evaluator, value any) (any, StepName, error) {
	switch v := value.(type) {
	case types.Exception:
		return nil, "", v

	case string:
		return nil, "", types.NewExceptionByString(v)

	case map[string]any:
		return nil, "", types.NewExceptionByMap(v)

	default:
		return nil, "", &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("invalid raise: must be string or map"),
		}
	}
}

type stepsStep struct {
	name         StepName
	steps        []AnonymousStep
	nextStepName StepName
}

func newStepsStep(stepName StepName, def anonymousStepDef, defaultNextStepName StepName) (*stepsStep, error) {
	var stepsDef []anonymousStepDef
	err := json.Unmarshal(def["steps"], stepsDef)
	if err != nil {
		return nil, fmt.Errorf("invalid steps: %w", err)
	}

	steps := make([]AnonymousStep, len(stepsDef))
	for i, stepDef := range stepsDef {
		steps[i], err = stepDef.compile("", "")
		if err != nil {
			return nil, fmt.Errorf("invalid steps[%d]: %w", i, err)
		}
	}

	nextStepName := defaultNextStepName
	if nextJSON, ok := def["next"]; ok {
		err = json.Unmarshal(nextJSON, &nextStepName)
		if err != nil {
			return nil, fmt.Errorf("invalid next %q", string(nextJSON))
		}
	}

	return &stepsStep{
		name:         stepName,
		steps:        steps,
		nextStepName: nextStepName,
	}, nil
}

func (s *stepsStep) Name() StepName {
	return s.name
}

func (s *stepsStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	for i, step := range s.steps {
		ret, nextStep, err := step.Execute(ev)
		if err != nil {
			return nil, "", fmt.Errorf("invalid condition[%d]: %w", i, err)
		}
		if nextStep != "" {
			return ret, nextStep, nil
		}
	}
	return nil, s.nextStepName, nil
}

type callStep struct {
	name         StepName
	call         *expression.Expr
	args         any
	result       *expression.Expr
	nextStepName StepName
}

func newCallStep(stepName StepName, def anonymousStepDef, defaultNextStepName StepName) (*callStep, error) {
	var call string
	err := json.Unmarshal(def["call"], &call)
	if err != nil {
		return nil, fmt.Errorf("invalid call %q", string(def["call"]))
	}

	var args any
	err = unmarshalJSONUseNumber(def["args"], &args)
	if err != nil {
		return nil, fmt.Errorf("invalid args %q", string(def["args"]))
	}
	args, err = decodeJSONNumberRecursive(args)
	if err != nil {
		return nil, fmt.Errorf("invalid number in args %q", string(def["args"]))
	}

	callExpr, err := expression.ParseExpr(call)
	if err != nil {
		return nil, fmt.Errorf("invalid call: %w", err)
	}
	if !callExpr.CanReference() {
		return nil, fmt.Errorf("invalid call: not callable %q", call)
	}

	args, err = expression.ExpandExprRecursive(args)
	if err != nil {
		return nil, fmt.Errorf("invalid args: %w", err)
	}
	switch args.(type) {
	case []any, map[string]any:
		// OK
	default:
		return nil, fmt.Errorf("invalid args type=%T: %+v", args, args)
	}

	var resultExpr *expression.Expr
	if resultJSON, ok := def["result"]; ok {
		var result string
		err = json.Unmarshal(resultJSON, &result)
		if err != nil {
			return nil, fmt.Errorf("invalid result %q", string(resultJSON))
		}

		resultExpr, err = expression.ParseExpr(result)
		if err != nil {
			return nil, fmt.Errorf("invalid result: %w", err)
		}
		if !resultExpr.IsSymbol() {
			return nil, fmt.Errorf("invalid result: must be a symbol but actual %q", result)
		}
	}

	nextStepName := defaultNextStepName
	if nextJSON, ok := def["next"]; ok {
		err = json.Unmarshal(nextJSON, &nextStepName)
		if err != nil {
			return nil, fmt.Errorf("invalid next %q", string(nextJSON))
		}
	}

	return &callStep{
		name:         stepName,
		call:         callExpr,
		args:         args,
		result:       resultExpr,
		nextStepName: nextStepName,
	}, nil
}

func (s *callStep) Name() StepName {
	return s.name
}

func (s *callStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	callRef, err := ev.ResolveReference(s.call)
	if err != nil {
		return nil, "", fmt.Errorf("unknown call %q: %w", s.call.Source, err)
	}

	callRaw, err := callRef.ResolveValue(ev.SymbolTable)
	if err != nil {
		return nil, "", fmt.Errorf("unknown call %q: %w", s.call.Source, err)
	}
	f, ok := callRaw.Get().(types.Function)
	if !ok {
		return nil, "", fmt.Errorf("not a callbale function: %s", s.call.Source)
	}

	argsRaw, err := ev.EvaluateValueRecursive(s.args)
	if err != nil {
		return nil, "", fmt.Errorf("invalid args: %w", err)
	}

	var args []any
	switch v := argsRaw.(type) {
	case []any:
		args = v
	case map[string]any:
		args = lo.Map(f.Args(), func(key string, _ int) any {
			return v[key]
		})
	default:
		panic(fmt.Sprintf("invalid args value: %T %+v", v, v))
	}

	var variable expression.Variable
	if s.result != nil {
		resultRef, err := ev.ResolveReference(s.result)
		if err != nil {
			return nil, "", fmt.Errorf("unknown result %q: %w", s.call.Source, err)
		}

		variable, err = resultRef.ResolveVariable(ev.SymbolTable)
		if err != nil {
			return nil, "", fmt.Errorf("unknown result %q: %w", s.call.Source, err)
		}
	}

	log.Printf("[DEBUG] call %s %+v", s.call.Source, argsRaw)
	ret, err := f.Call(args)
	if err != nil {
		return nil, "", fmt.Errorf("call %q: %w", s.call.Source, err)
	}
	if variable != nil {
		variable.Set(ret)
	}

	return ret, s.nextStepName, nil
}

type switchStep struct {
	name                StepName
	conditions          []switchCondition
	defaultStep         AnonymousStep
	defaultNextStepName StepName
}

type switchCondition struct {
	condition *expression.Expr
	step      AnonymousStep
}

func newSwitchStep(stepName StepName, def anonymousStepDef, defaultNextStepName StepName) (*switchStep, error) {
	var switchStepDefs []anonymousStepDef
	if err := json.Unmarshal(def["switch"], &switchStepDefs); err != nil {
		return nil, fmt.Errorf("invalid switch: %w", err)
	}

	var defaultStep AnonymousStep
	conditions := make([]switchCondition, len(switchStepDefs))
	for i, switchStepDef := range switchStepDefs {
		conditionDefRaw := switchStepDef["condition"]
		delete(switchStepDef, "condition")

		var err error
		conditions[i].step, err = switchStepDef.compile("", "")
		if err != nil {
			return nil, fmt.Errorf("invalid switch[%d]: %w", i, err)
		}

		var conditionDef string
		if err = json.Unmarshal(conditionDefRaw, &conditionDef); err != nil {
			return nil, fmt.Errorf("invalid switch[%d].condition: %w", i, err)
		}

		if expr := expression.TrimExprParen(conditionDef); expr != conditionDef {
			conditions[i].condition, err = expression.ParseExpr(expr)
			if err != nil {
				return nil, fmt.Errorf("invalid switch[%d].condition: %w", i, err)
			}
		} else if conditionDef == "true" {
			if i != len(switchStepDefs)-1 {
				return nil, fmt.Errorf("invalid switch[%d].condition: default step must be last", i)
			}
			defaultStep = conditions[i].step
			conditions = conditions[:i]
		} else {
			return nil, fmt.Errorf("invalid switch[%d].condition: unknown format %q", i, conditionDef)
		}
	}

	return &switchStep{
		name:                stepName,
		conditions:          conditions,
		defaultStep:         defaultStep,
		defaultNextStepName: defaultNextStepName,
	}, nil
}

func (s *switchStep) Name() StepName {
	return s.name
}

func (s *switchStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	for i, c := range s.conditions {
		ret, err := ev.EvaluateValue(c.condition)
		if err != nil {
			return nil, "", fmt.Errorf("invalid condition[%d]: %w", i, err)
		}

		if ok, isBool := ret.(bool); isBool && ok {
			ret, nextStepName, err := c.step.Execute(ev)
			if err != nil {
				return nil, "", err
			}

			if nextStepName == "" {
				nextStepName = s.defaultNextStepName
			}
			return ret, nextStepName, nil
		}
	}

	if s.defaultStep != nil {
		ret, nextStepName, err := s.defaultStep.Execute(ev)
		if err != nil {
			return nil, "", err
		}

		if nextStepName == "" {
			nextStepName = s.defaultNextStepName
		}
		return ret, nextStepName, nil
	}

	return nil, s.defaultNextStepName, nil
}

type tryStep struct {
	name                StepName
	realStep            AnonymousStep
	retryPolicy         *expression.Expr
	exceptStep          *exceptStep
	defaultNextStepName StepName
}

type retryPolicyDef struct {
	Predicate  string                 `json:"predicate" mapstructure:"predicate"`
	MaxRetries int                    `json:"max_retries" mapstructure:"max_retries"`
	Backoff    *retryBackoffPolicyDef `json:"backoff" mapstructure:"backoff"`
}

func (p *retryPolicyDef) compile() (*retryPolicy, error) {
	if p.Predicate == "" {
		return nil, fmt.Errorf("predecate: required")
	}

	policy := &retryPolicy{
		maxRetries: p.MaxRetries,
		backoff:    p.Backoff.compile(),
	}
	if expr := expression.TrimExprParen(p.Predicate); expr != p.Predicate {
		predecate, err := expression.ParseExpr(expr)
		if err != nil {
			return nil, fmt.Errorf("predecate: %w", err)
		}
		if !predecate.CanReference() {
			return nil, fmt.Errorf("predecate: cannot reference to %q", expr)
		}

		policy.predicate = predecate
	} else {
		return nil, fmt.Errorf("predecate: not a expression")
	}

	// set default for retry policy
	if policy.maxRetries == 0 {
		policy.maxRetries = 5
	}
	if policy.backoff == nil {
		policy.backoff = &retryBackoffPolicy{
			initialDelay: 1 * time.Second,
			maxDelay:     60 * time.Second,
			multiplier:   1.25,
		}
	}
	return policy, nil
}

type retryBackoffPolicyDef struct {
	InitialDelay float64 `json:"initial_delay" mapstructure:"initial_delay"`
	MaxDelay     float64 `json:"max_delay" mapstructure:"max_delay"`
	Multiplier   float64 `json:"max_dmultiplierelay" mapstructure:"max_dmultiplierelay"`
}

func (p *retryBackoffPolicyDef) compile() *retryBackoffPolicy {
	if p == nil {
		return nil
	}

	return &retryBackoffPolicy{
		initialDelay: time.Duration(time.Second * time.Duration(p.InitialDelay)),
		maxDelay:     time.Duration(time.Second * time.Duration(p.MaxDelay)),
		multiplier:   p.Multiplier,
	}
}

type retryPolicy struct {
	predicate  *expression.Expr
	maxRetries int
	backoff    *retryBackoffPolicy
}

type retryBackoffPolicy struct {
	initialDelay time.Duration
	maxDelay     time.Duration
	multiplier   float64
}

func newTryStep(stepName StepName, def anonymousStepDef, defaultNextStepName StepName) (*tryStep, error) {
	var realStep AnonymousStep
	{
		var tryStepDef anonymousStepDef
		err := json.Unmarshal(def["try"], &tryStepDef)
		if err != nil {
			return nil, fmt.Errorf("invalid try: %w", err)
		}

		realStep, err = tryStepDef.compile("", "")
		if err != nil {
			return nil, fmt.Errorf("invalid try: %w", err)
		}
	}

	var retry *expression.Expr
	if retryBytes, ok := def["retry"]; ok {
		var retryPolicyDef retryPolicyDef
		err := json.Unmarshal(retryBytes, &retryPolicyDef)
		if err == nil {
			retryPolicy, err := retryPolicyDef.compile()
			if err != nil {
				return nil, fmt.Errorf("invalid retry: %w", err)
			}
			retry = expression.ValueExpr(retryPolicy)
		} else {
			var retryExpr string
			if err = json.Unmarshal(retryBytes, &retryExpr); err != nil {
				return nil, fmt.Errorf("invalid retry: %w", err)
			}
			if expr := expression.TrimExprParen(retryExpr); expr != retryExpr {
				retry, err = expression.ParseExpr(expr)
				if err != nil {
					return nil, fmt.Errorf("invalid retry: %w", err)
				}
			} else {
				return nil, fmt.Errorf("invalid retry: %w", err)
			}
		}
	}

	var except *exceptStep
	if exceptBytes, ok := def["except"]; ok {
		var err error
		except, err = newExceptStep(exceptBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid except: %w", err)
		}
	}

	return &tryStep{
		name:                stepName,
		realStep:            realStep,
		retryPolicy:         retry,
		exceptStep:          except,
		defaultNextStepName: defaultNextStepName,
	}, nil
}

func (s *tryStep) Name() StepName {
	return s.name
}

func (s *tryStep) evaluateRetryPolicy(ev *expression.Evaluator) (*retryPolicy, error) {
	retryAny, err := ev.EvaluateValue(s.retryPolicy)
	if err != nil {
		return nil, err
	}

	var policy *retryPolicy
	switch v := retryAny.(type) {
	case *retryPolicy:
		policy = v

	case map[string]any:
		var policyDef retryPolicyDef
		err = mapstructure.Decode(v, &policyDef)
		if err != nil {
			return nil, err
		}
		policy, err = policyDef.compile()
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown type: %T", retryAny)
	}

	return policy, nil
}

func (s *tryStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	if s.retryPolicy == nil {
		return s.execute(ev, nil)
	}

	policy, err := s.evaluateRetryPolicy(ev)
	if err != nil {
		return nil, "", fmt.Errorf("retry: %w", err)
	}

	return s.execute(ev, &retryStatus{
		restRetries: policy.maxRetries,
		delay:       policy.backoff.initialDelay,
		policy:      policy,
	})
}

type retryStatus struct {
	delay       time.Duration
	restRetries int
	policy      *retryPolicy
}

func (s *tryStep) execute(ev *expression.Evaluator, retry *retryStatus) (any, StepName, error) {
	ret, nextStepName, err := s.realStep.Execute(ev)
	if err == nil {
		if nextStepName == "" {
			nextStepName = s.defaultNextStepName
		}
		return ret, nextStepName, nil
	}
	pp.Println(err)

	var exception types.Exception
	if !errors.As(err, &exception) {
		return nil, "", err
	}
	if retry != nil && retry.restRetries > 0 {
		predicate, err := ev.EvaluateValue(retry.policy.predicate)
		if err != nil {
			panic(err)
		}

		result, err := predicate.(types.Function).Call([]any{exception.Exception()})
		if err != nil {
			panic(err)
		}

		if result.(bool) {
			time.Sleep(retry.delay)
			retry.delay = time.Duration(float64(retry.delay) * retry.policy.backoff.multiplier)
			if retry.delay > retry.policy.backoff.maxDelay {
				retry.delay = retry.policy.backoff.maxDelay
			}
			retry.restRetries--
			return s.execute(ev, retry)
		}
	}
	if s.exceptStep == nil {
		return nil, "", err
	}

	ret, nextStepName, err = s.exceptStep.execute(ev.SymbolTable, exception)
	if err != nil {
		return nil, "", err
	}

	if nextStepName == "" {
		nextStepName = s.defaultNextStepName
	}
	return ret, nextStepName, nil
}

func newExceptStep(def json.RawMessage) (*exceptStep, error) {
	var stepDef struct {
		As    string          `json:"as"`
		Steps json.RawMessage `json:"steps"`
	}
	if err := json.Unmarshal(def, &stepDef); err != nil {
		return nil, err
	}
	if stepDef.As == "" {
		return nil, fmt.Errorf("as: required")
	}

	as, err := expression.ParseExpr(stepDef.As)
	if err != nil {
		return nil, fmt.Errorf("as: %w", err)
	}
	if !as.IsSymbol() {
		return nil, fmt.Errorf("as: not a symbol %q", stepDef.As)
	}

	steps, err := newStepsStep("", anonymousStepDef{
		"steps": stepDef.Steps,
	}, "")
	if err != nil {
		return nil, fmt.Errorf("steps: %w", err)
	}

	return &exceptStep{
		as:    as,
		steps: steps,
	}, nil
}

type exceptStep struct {
	as    *expression.Expr
	steps *stepsStep
}

func (s *exceptStep) execute(symbolTable types.SymbolTable, exception types.Exception) (any, StepName, error) {
	evaluator := expression.Evaluator{SymbolTable: symbolTable.ShallowClone()}
	ref, err := evaluator.ResolveReference(s.as)
	if err != nil {
		panic(err)
	}

	variable, err := ref.ResolveVariable(evaluator.SymbolTable)
	if err != nil {
		panic(err)
	}
	variable.Set(exception.Exception())

	ret, nextStepName, err := s.steps.Execute(&evaluator)
	if err != nil {
		return nil, "", err
	}

	return ret, nextStepName, nil
}
