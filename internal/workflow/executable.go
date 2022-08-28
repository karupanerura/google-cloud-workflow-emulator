package workflow

import (
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/defaults"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/expression"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type WorkflowRoot map[string]*Workflow

func (r WorkflowRoot) Execute(args any) (any, error) {
	mainWorkflow, ok := r["main"]
	if !ok {
		return nil, fmt.Errorf("main workflow is not defined")
	}

	st := &types.SymbolTable{
		Symbols: map[string]any{},
		Parent:  defaults.DefaultSymbolTable,
	}
	for name, workflow := range r {
		if name == "main" {
			continue
		}

		name := name
		workflow := workflow
		st.Symbols[name] = types.NewRawFunction(name, workflow.Params, func(args []any) (any, error) {
			st := &types.SymbolTable{
				Symbols: map[string]any{},
				Parent:  defaults.DefaultSymbolTable,
			}
			for i, param := range workflow.Params {
				st.Symbols[param.Name] = args[i]
			}
			return workflow.Execute(st)
		})
	}

	if len(mainWorkflow.Params) == 1 {
		st.Symbols[mainWorkflow.Params[0].Name] = args
	}
	return mainWorkflow.Execute(st)
}

type Workflow struct {
	Name   string
	Params []types.Argument

	entryStep Step
	stepMap   map[StepName]Step
}

func (w *Workflow) Execute(symbolTable *types.SymbolTable) (ret any, err error) {
	for _, param := range w.Params {
		if _, ok := symbolTable.Symbols[param.Name]; ok {
			continue
		}
		if param.Optional {
			continue
		}
		if param.Default != nil {
			symbolTable.Symbols[param.Name] = param.Default
		}
		return nil, fmt.Errorf("missing param: %s", param.Name)
	}

	ev := expression.Evaluator{SymbolTable: symbolTable}
	step := w.entryStep
	for step != nil {
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

type namedStep struct {
	name StepName
	step AnonymousStep
	next StepName
}

func (s *namedStep) Name() StepName {
	return s.name
}

func (s *namedStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	ret, nextStep, err := s.step.Execute(ev)
	if err != nil {
		return nil, "", err
	}

	if nextStep == "" {
		nextStep = s.next
	}
	return ret, nextStep, nil
}

type assignStep struct {
	assigns []assignOperation
}

type assignOperation struct {
	left  *expression.Expr
	right any
}

func newAssignStep(def anonymousStepDef) (*assignStep, error) {
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
		assigns: assigns,
	}, nil
}

func (s *assignStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	var inheritedVariables *types.InternalInheritedVariables
	if v, ok := ev.SymbolTable.Get(types.InternalInheritedVariablesSymbol); ok {
		inheritedVariables = v.(*types.InternalInheritedVariables)

		exprs := lo.Map(s.assigns, func(assign assignOperation, _ int) *expression.Expr {
			return assign.left
		})

		unlock, err := ev.LockSharedVariablesIfNeeded(exprs...)
		if err != nil {
			return nil, "", fmt.Errorf("LockSharedVariablesIfNeeded: %w", err)
		}
		defer unlock()
	}

	for i, assign := range s.assigns {
		ref, err := ev.ResolveReference(assign.left)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}
		variable, err := ref.ResolveVariable(ev.SymbolTable)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}
		if inheritedVariables != nil {
			rootSym, _ := variable.Paths()
			if _, inherited := inheritedVariables.Shared[rootSym]; inherited {
				return nil, "", fmt.Errorf("invalid assign[%d]: cannot assign to non-shared variable in parallel step", i)
			}
		}

		value, err := ev.EvaluateValueRecursive(assign.right)
		if err != nil {
			return nil, "", fmt.Errorf("invalid assign[%d]: %w", i, err)
		}
		variable.Set(value)
	}
	return nil, "", nil
}

type returnStep struct {
	returnValue any
}

func newReturnStep(def anonymousStepDef) (*returnStep, error) {
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
		returnValue: returnValue,
	}, nil
}

func (s *returnStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	ret, err := ev.EvaluateValueRecursive(s.returnValue)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", s.returnValue, err)
	}

	return ret, "end", nil
}

type raiseStep struct {
	raiseValue any
}

func newRaiseStep(def anonymousStepDef) (*raiseStep, error) {
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
		raiseValue: raiseValue,
	}, nil
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

type anonymousStepsStep struct {
	steps []AnonymousStep
}

func newAnonymousStepsStep(def anonymousStepDef) (*anonymousStepsStep, error) {
	var stepsDef []anonymousStepDef
	err := json.Unmarshal(def["steps"], stepsDef)
	if err != nil {
		return nil, fmt.Errorf("invalid steps: %w", err)
	}

	steps := make([]AnonymousStep, len(stepsDef))
	for i, stepDef := range stepsDef {
		steps[i], err = stepDef.compile()
		if err != nil {
			return nil, fmt.Errorf("invalid steps[%d]: %w", i, err)
		}
	}

	return &anonymousStepsStep{
		steps: steps,
	}, nil
}

func (s *anonymousStepsStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	for i, step := range s.steps {
		ret, nextStep, err := step.Execute(ev)
		if err != nil {
			return nil, "", fmt.Errorf("invalid condition[%d]: %w", i, err)
		}
		if nextStep != "" {
			return ret, nextStep, nil
		}
	}
	return nil, "", nil
}

type callStep struct {
	call   *expression.Expr
	args   any
	result *expression.Expr
}

func newCallStep(def anonymousStepDef) (*callStep, error) {
	var call string
	err := json.Unmarshal(def["call"], &call)
	if err != nil {
		return nil, fmt.Errorf("invalid call %q", string(def["call"]))
	}

	var args any
	if argsDef, ok := def["args"]; ok {
		err = unmarshalJSONUseNumber(argsDef, &args)
		if err != nil {
			return nil, fmt.Errorf("invalid args %q", string(argsDef))
		}
		args, err = decodeJSONNumberRecursive(args)
		if err != nil {
			return nil, fmt.Errorf("invalid number in args %q", string(argsDef))
		}
	} else {
		args = []any{}
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

	return &callStep{
		call:   callExpr,
		args:   args,
		result: resultExpr,
	}, nil
}

func (s *callStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	if _, ok := ev.SymbolTable.Get(types.InternalInheritedVariablesSymbol); ok && s.result != nil {
		unlock, err := ev.LockSharedVariablesIfNeeded(s.result)
		if err != nil {
			return nil, "", fmt.Errorf("LockSharedVariablesIfNeeded: %w", err)
		}
		defer unlock()
	}

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
		return nil, "", fmt.Errorf("not a callable function: %s", s.call.Source)
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
			value, ok := v[key]
			if ok {
				return value
			}
			return types.SubstitutionNone
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

	ret, err := f.Call(args)
	if err != nil {
		return nil, "", fmt.Errorf("call %q: %w", s.call.Source, err)
	}
	if variable != nil {
		variable.Set(ret)
	}

	return ret, "", nil
}

type switchStep struct {
	conditions  []switchCondition
	defaultStep AnonymousStep
}

type switchCondition struct {
	condition *expression.Expr
	step      AnonymousStep
}

func newSwitchStep(def anonymousStepDef) (*switchStep, error) {
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
		conditions[i].step, err = switchStepDef.compile()
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
		conditions:  conditions,
		defaultStep: defaultStep,
	}, nil
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

			return ret, nextStepName, nil
		}
	}

	if s.defaultStep != nil {
		ret, nextStepName, err := s.defaultStep.Execute(ev)
		if err != nil {
			return nil, "", err
		}

		return ret, nextStepName, nil
	}

	return nil, "", nil
}

type tryStep struct {
	realStep    AnonymousStep
	retryPolicy *expression.Expr
	exceptStep  *exceptStep
}

type retryPolicyDef struct {
	Predicate  string                 `json:"predicate" mapstructure:"predicate"`
	MaxRetries int                    `json:"max_retries" mapstructure:"max_retries"`
	Backoff    *retryBackoffPolicyDef `json:"backoff" mapstructure:"backoff"`
}

func (p *retryPolicyDef) compile() (*retryPolicy, error) {
	if p.Predicate == "" {
		return nil, fmt.Errorf("predicate: required")
	}

	policy := &retryPolicy{
		maxRetries: p.MaxRetries,
		backoff:    p.Backoff.compile(),
	}
	if expr := expression.TrimExprParen(p.Predicate); expr != p.Predicate {
		predicate, err := expression.ParseExpr(expr)
		if err != nil {
			return nil, fmt.Errorf("predicate: %w", err)
		}
		if !predicate.CanReference() {
			return nil, fmt.Errorf("predicate: cannot reference to %q", expr)
		}

		policy.predicate = predicate
	} else {
		return nil, fmt.Errorf("predicate: not a expression")
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
	Multiplier   float64 `json:"multiplier" mapstructure:"multiplier"`
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

func newTryStep(def anonymousStepDef) (*tryStep, error) {
	var realStep AnonymousStep
	{
		var tryStepDef anonymousStepDef
		err := json.Unmarshal(def["try"], &tryStepDef)
		if err != nil {
			return nil, fmt.Errorf("invalid try: %w", err)
		}

		realStep, err = tryStepDef.compile()
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
		realStep:    realStep,
		retryPolicy: retry,
		exceptStep:  except,
	}, nil
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
		return ret, nextStepName, nil
	}

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

	return s.exceptStep.execute(ev.SymbolTable, exception)
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

	steps, err := newAnonymousStepsStep(anonymousStepDef{
		"steps": stepDef.Steps,
	})
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
	steps *anonymousStepsStep
}

func (s *exceptStep) execute(symbolTable *types.SymbolTable, exception types.Exception) (any, StepName, error) {
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

func newForStep(def anonymousStepDef, parallel *parallelPolicy) (*forStep, error) {
	type forStepDef struct {
		Value string             `json:"value"`
		In    any                `json:"in"`
		Steps []*workflowStepDef `json:"steps"`
	}

	var decoded forStepDef
	if err := unmarshalJSONUseNumber(def["for"], &decoded); err != nil {
		return nil, fmt.Errorf("invalid for: %w", err)
	}

	var err error
	decoded.In, err = decodeJSONNumberRecursive(decoded.In)
	if err != nil {
		return nil, fmt.Errorf("invalid for.in: %w", err)
	}
	decoded.In, err = expression.ExpandExprRecursive(decoded.In)
	if err != nil {
		return nil, fmt.Errorf("invalid for.in: %w", err)
	}
	switch decoded.In.(type) {
	case []any, *expression.Expr:
		// ok
	default:
		return nil, fmt.Errorf("invalid for.in: must be an array or expression")
	}

	// parse steps
	wf := &forStepsWorkflow{
		stepMap: make(map[StepName]Step, len(decoded.Steps)),
	}
	for i, stepDef := range decoded.Steps {
		if _, duplicated := wf.stepMap[stepDef.name]; duplicated {
			return nil, fmt.Errorf("%s: duplicated step name in steps", stepDef.name)
		}

		var defaultNextStepName StepName
		if i == len(decoded.Steps)-1 {
			defaultNextStepName = "continue"
		} else {
			defaultNextStepName = decoded.Steps[i+1].name
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

	return &forStep{
		value:    decoded.Value,
		in:       decoded.In,
		workflow: wf,
		parallel: parallel,
	}, nil
}

type forStep struct {
	value    string
	in       any
	workflow *forStepsWorkflow
	parallel *parallelPolicy
}

func (s *forStep) Execute(ev *expression.Evaluator) (any, StepName, error) {
	if s.parallel != nil {
		return s.executeInParallel(ev)
	}
	return s.executeInSerial(ev)
}

func (s *forStep) executeInSerial(ev *expression.Evaluator) (any, StepName, error) {
	inAny, err := ev.EvaluateValueRecursive(s.in)
	if err != nil {
		return nil, "", fmt.Errorf("in: %w", err)
	}

	in, ok := inAny.([]any)
	if !ok {
		return nil, "", &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("in: must be an array"),
		}
	}

	for i, v := range in {
		symbolTable := &types.SymbolTable{
			Symbols: map[string]any{
				s.value: v,
			},
			Parent: ev.SymbolTable,
		}

		ctrl, err := s.workflow.execute(symbolTable)
		if err != nil {
			return nil, "", fmt.Errorf("in[%d]: %w", i, err)
		}

		if ctrl == breakForStepLoopControl {
			break
		} else if ctrl == continueForStepLoopControl {
			continue
		}

		panic("unknown loop control without error")
	}

	return nil, "", nil
}

func (s *forStep) executeInParallel(ev *expression.Evaluator) (any, StepName, error) {
	inAny, err := ev.EvaluateValueRecursive(s.in)
	if err != nil {
		return nil, "", fmt.Errorf("in: %w", err)
	}

	in, ok := inAny.([]any)
	if !ok {
		return nil, "", &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("in: must be an array"),
		}
	}

	symbolTable := ev.SymbolTable.ShallowClone()
	inheritedVariables := &types.InternalInheritedVariables{
		Shared: make(map[string]bool, len(symbolTable.Symbols)),
	}
	for key := range symbolTable.KeysChan() {
		inheritedVariables.Shared[key] = false
	}
	for i, shared := range s.parallel.shared {
		ref, err := ev.ResolveReference(shared)
		if err != nil {
			return nil, "", fmt.Errorf("invalid shared[%d]: %w", i, err)
		}

		v, err := ref.ResolveVariable(symbolTable)
		if err != nil {
			return nil, "", fmt.Errorf("invalid shared[%d]: %w", i, err)
		}

		value := v.Get()
		v.Set(&types.SharedVariable{Value: value})

		root, _ := v.Paths()
		inheritedVariables.Shared[root] = true
	}
	symbolTable.Symbols[types.InternalInheritedVariablesSymbol] = inheritedVariables

	eg := errgroup.Group{}
	for i, v := range in {
		i := i
		v := v
		eg.Go(func() error {
			symbolTable := &types.SymbolTable{
				Symbols: map[string]any{
					s.value: v,
				},
				Parent: symbolTable,
			}

			ctrl, err := s.workflow.execute(symbolTable)
			if err != nil {
				return fmt.Errorf("in[%d]: %w", i, err)
			}
			if ctrl == continueForStepLoopControl {
				return nil
			}

			return nil
		})
	}
	return nil, "", eg.Wait()
}

type forStepLoopControl int

const (
	unknownForStepLoopControl forStepLoopControl = iota
	continueForStepLoopControl
	breakForStepLoopControl
)

type forStepsWorkflow struct {
	entryStep Step
	stepMap   map[StepName]Step
}

func (w *forStepsWorkflow) execute(symbolTable *types.SymbolTable) (forStepLoopControl, error) {
	ev := expression.Evaluator{SymbolTable: symbolTable}
	step := w.entryStep
	for step != nil {
		_, nextStepName, err := step.Execute(&ev)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", step.Name(), err)
		}
		if nextStepName == "break" {
			return breakForStepLoopControl, nil
		} else if nextStepName == "continue" {
			return continueForStepLoopControl, nil
		} else if nextStepName == "" {
			return 0, fmt.Errorf("%s: next step is not defined", step.Name())
		}

		nextStep, ok := w.stepMap[nextStepName]
		if !ok {
			return 0, fmt.Errorf("%s: not found", nextStepName)
		}

		step = nextStep
	}

	return continueForStepLoopControl, nil
}

func newParallelStep(def anonymousStepDef) (AnonymousStep, error) {
	var parallelDef map[string]json.RawMessage
	if err := json.Unmarshal(def["parallel"], &parallelDef); err != nil {
		return nil, fmt.Errorf("parallel: %w", err)
	}

	exceptionPolicy := "continueAll"
	if exceptionPolicyDef, ok := parallelDef["exception_policy"]; ok {
		if err := json.Unmarshal(exceptionPolicyDef, &exceptionPolicy); err != nil {
			return nil, fmt.Errorf("parallel: invalid exception_policy: %w", err)
		}
		if exceptionPolicy != "continueAll" {
			return nil, fmt.Errorf("parallel: unsupported exception_policy: %s", exceptionPolicyDef)
		}
	}

	var sharedDef []string
	if err := json.Unmarshal(parallelDef["shared"], &sharedDef); err != nil {
		return nil, fmt.Errorf("parallel: invalid shared: %w", err)
	}

	shared := make([]*expression.Expr, len(sharedDef))
	for i, def := range sharedDef {
		var err error
		shared[i], err = expression.ParseExpr(def)
		if err != nil {
			return nil, fmt.Errorf("parallel: invalid shared[%d]: %w", i, err)
		}
		if !shared[i].IsField() {
			return nil, fmt.Errorf("parallel: invalid shared[%d]: must be a variable", i)
		}
	}

	policy := &parallelPolicy{
		exceptionPolicy: exceptionPolicy,
		shared:          shared,
	}

	var step AnonymousStep
	if parallelDef["for"] != nil && parallelDef["branches"] != nil {
		return nil, fmt.Errorf("parallel: only specify `for` or `branches` either")
	} else if parallelDef["for"] != nil {
		var err error
		step, err = newForStep(parallelDef, policy)
		if err != nil {
			return nil, fmt.Errorf("parallel: %w", err)
		}
	} else if parallelDef["branches"] != nil {
		panic("TODO")
	} else {
		return nil, fmt.Errorf("parallel: must specify `for` or `branches`")
	}

	return step, nil
}

type parallelPolicy struct {
	exceptionPolicy string
	shared          []*expression.Expr
}
