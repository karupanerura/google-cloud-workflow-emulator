package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/k0kubun/pp"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type Reference interface {
	ResolveVariable(types.SymbolTable) (Variable, error)
	ResolveValue(types.SymbolTable) (Value, error)
}

type Value interface {
	Path() string
	Get() any
}

type pureValue struct {
	path string
	body any
}

var _ Value = (*pureValue)(nil)

func (v *pureValue) Path() string {
	return v.path
}

func (v *pureValue) Get() any {
	return v.body
}

type Variable interface {
	Value
	Set(any)
}

type pureVariable struct {
	path   string
	getter func() any
	setter func(any)
}

var _ Variable = (*pureVariable)(nil)

func (v *pureVariable) Path() string {
	return v.path
}

func (v *pureVariable) Get() any {
	return v.getter()
}

func (v *pureVariable) Set(value any) {
	v.setter(value)
}

type symbolReference struct {
	name string
}

func (r *symbolReference) ResolveVariable(st types.SymbolTable) (Variable, error) {
	return &pureVariable{
		path: r.name,
		getter: func() any {
			return st[r.name]
		},
		setter: func(value any) {
			st[r.name] = value
		},
	}, nil
}

func (r *symbolReference) ResolveValue(st types.SymbolTable) (Value, error) {
	if _, ok := st[r.name]; !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("not found symbol: %s", r.name),
		}
	}

	v, err := r.ResolveVariable(st)
	if err != nil {
		return nil, err
	}

	return &pureValue{
		path: v.Path(),
		body: v.Get(),
	}, nil
}

type fieldReference struct {
	context Reference
	name    string
}

func (r *fieldReference) resolvePath(context Value) string {
	var b strings.Builder
	b.WriteString(context.Path())
	if strings.ContainsAny(r.name, `. ,+-*/%"\\!=><()[]`+"\t\n") {
		b.WriteByte('[')
		b.WriteString(strconv.Quote(r.name))
		b.WriteByte(']')
	} else {
		b.WriteByte('.')
		b.WriteString(r.name)
	}

	return b.String()
}

func (r *fieldReference) ResolveVariable(st types.SymbolTable) (Variable, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	path := r.resolvePath(contextRef)
	context, ok := contextRef.Get().(map[string]any)
	if !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}

	return &pureVariable{
		path: path,
		getter: func() any {
			return context[r.name]
		},
		setter: func(value any) {
			pp.Println("SET ", value)
			context[r.name] = value
		},
	}, nil
}

func (r *fieldReference) ResolveValue(st types.SymbolTable) (Value, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	path := r.resolvePath(contextRef)
	context, ok := contextRef.Get().(map[string]any)
	if !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}
	if _, ok = context[r.name]; !ok {
		return nil, &types.Error{
			Tag: types.KeyErrorTag,
			Err: fmt.Errorf("%s: not found", path),
		}
	}

	return &pureValue{
		path: path,
		body: context[r.name],
	}, nil
}

type indexReference struct {
	context Reference
	index   int64
}

func (r *indexReference) resolvePath(context Value) string {
	var b strings.Builder
	b.WriteString(context.Path())
	b.WriteByte('[')
	b.WriteString(strconv.FormatInt(r.index, 10))
	b.WriteByte(']')
	return b.String()
}

func (r *indexReference) ResolveVariable(st types.SymbolTable) (Variable, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	path := r.resolvePath(contextRef)
	context, ok := contextRef.Get().([]any)
	if !ok {
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}
	if r.index >= int64(len(context)) {
		return nil, &types.Error{
			Tag: types.IndexErrorTag,
			Err: fmt.Errorf("%s: array index %d out of bounds", path, r.index),
		}
	}

	return &pureVariable{
		path: path,
		getter: func() any {
			return context[r.index]
		},
		setter: func(value any) {
			context[r.index] = value
		},
	}, nil
}

func (r *indexReference) ResolveValue(st types.SymbolTable) (Value, error) {
	v, err := r.ResolveVariable(st)
	if err != nil {
		return nil, err
	}

	return &pureValue{
		path: v.Path(),
		body: v.Get(),
	}, nil
}
