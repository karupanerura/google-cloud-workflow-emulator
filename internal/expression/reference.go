package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type Reference interface {
	ResolveVariable(*types.SymbolTable) (Variable, error)
	ResolveValue(*types.SymbolTable) (Value, error)
}

type Value interface {
	Paths() (string, []any)
	Path() string
	Get() any
}

type pureValue struct {
	getPaths func() (string, []any)
	getPath  func() string
	body     any
}

var _ Value = (*pureValue)(nil)

func (v *pureValue) Paths() (string, []any) {
	return v.getPaths()
}

func (v *pureValue) Path() string {
	return v.getPath()
}

func (v *pureValue) Get() any {
	return v.body
}

type Variable interface {
	Value
	Set(any)
}

type pureVariable struct {
	getPaths func() (string, []any)
	getPath  func() string
	getter   func() any
	setter   func(any)
}

var _ Variable = (*pureVariable)(nil)

func (v *pureVariable) Paths() (string, []any) {
	return v.getPaths()
}

func (v *pureVariable) Path() string {
	return v.getPath()
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

func (r *symbolReference) ResolveVariable(st *types.SymbolTable) (Variable, error) {
	if v, ok := st.Get(r.name); ok {
		if vv, shared := v.(*types.SharedVariable); shared {
			return &pureVariable{
				getPath: func() string {
					return r.name
				},
				getPaths: func() (string, []any) {
					return r.name, nil
				},
				getter: func() any {
					vv.RLock()
					defer vv.RUnlock()
					return vv.Value
				},
				setter: func(value any) {
					vv.Value = value
				},
			}, nil
		}
	}

	return &pureVariable{
		getPath: func() string {
			return r.name
		},
		getPaths: func() (string, []any) {
			return r.name, nil
		},
		getter: func() any {
			v, _ := st.Get(r.name)
			return v
		},
		setter: func(value any) {
			st.Set(r.name, value)
		},
	}, nil
}

func (r *symbolReference) ResolveValue(st *types.SymbolTable) (Value, error) {
	if _, ok := st.Get(r.name); !ok {
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
		getPath: func() string {
			return r.name
		},
		getPaths: func() (string, []any) {
			return r.name, nil
		},
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

func (r *fieldReference) ResolveVariable(st *types.SymbolTable) (Variable, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	context, ok := contextRef.Get().(map[string]any)
	if !ok {
		path := r.resolvePath(contextRef)
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}

	return &pureVariable{
		getPath: func() string {
			return r.resolvePath(contextRef)
		},
		getPaths: func() (string, []any) {
			root, paths := contextRef.Paths()
			return root, append(paths, r.name)
		},
		getter: func() any {
			return context[r.name]
		},
		setter: func(value any) {
			context[r.name] = value
		},
	}, nil
}

func (r *fieldReference) ResolveValue(st *types.SymbolTable) (Value, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	context, ok := contextRef.Get().(map[string]any)
	if !ok {
		path := r.resolvePath(contextRef)
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}
	if _, ok = context[r.name]; !ok {
		path := r.resolvePath(contextRef)
		return nil, &types.Error{
			Tag: types.KeyErrorTag,
			Err: fmt.Errorf("%s: not found", path),
		}
	}

	if v, shared := context[r.name].(*types.SharedVariable); shared {
		v.RLock()
		defer v.RUnlock()
		return &pureValue{
			getPath: func() string {
				return r.resolvePath(contextRef)
			},
			getPaths: func() (string, []any) {
				root, paths := contextRef.Paths()
				return root, append(paths, r.name)
			},
			body: v.Value,
		}, nil
	}
	return &pureValue{
		getPath: func() string {
			return r.resolvePath(contextRef)
		},
		getPaths: func() (string, []any) {
			root, paths := contextRef.Paths()
			return root, append(paths, r.name)
		},
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

func (r *indexReference) ResolveVariable(st *types.SymbolTable) (Variable, error) {
	contextRef, err := r.context.ResolveValue(st)
	if err != nil {
		return nil, err
	}

	context, ok := contextRef.Get().([]any)
	if !ok {
		path := r.resolvePath(contextRef)
		return nil, &types.Error{
			Tag: types.TypeErrorTag,
			Err: fmt.Errorf("%s: unexpected context type %T at %s", path, contextRef.Get(), contextRef.Path()),
		}
	}
	if r.index >= int64(len(context)) {
		path := r.resolvePath(contextRef)
		return nil, &types.Error{
			Tag: types.IndexErrorTag,
			Err: fmt.Errorf("%s: array index %d out of bounds", path, r.index),
		}
	}

	return &pureVariable{
		getPath: func() string {
			return r.resolvePath(contextRef)
		},
		getPaths: func() (string, []any) {
			root, paths := contextRef.Paths()
			return root, append(paths, r.index)
		},
		getter: func() any {
			return context[r.index]
		},
		setter: func(value any) {
			context[r.index] = value
		},
	}, nil
}

func (r *indexReference) ResolveValue(st *types.SymbolTable) (Value, error) {
	v, err := r.ResolveVariable(st)
	if err != nil {
		return nil, err
	}

	return &pureValue{
		getPath:  v.(*pureVariable).getPath,
		getPaths: v.(*pureVariable).getPaths,
		body:     v.Get(),
	}, nil
}
