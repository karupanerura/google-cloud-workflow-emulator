package types

import (
	"fmt"
	"strings"

	reflect "github.com/goccy/go-reflect"
	"github.com/samber/lo"
)

type Function interface {
	Name() string
	Args() []string
	Call([]any) (any, error)
}

var nonNilableTypeSet = map[reflect.Kind]bool{
	reflect.Bool:          true,
	reflect.Int:           true,
	reflect.Int8:          true,
	reflect.Int16:         true,
	reflect.Int32:         true,
	reflect.Int64:         true,
	reflect.Uint:          true,
	reflect.Uint8:         true,
	reflect.Uint16:        true,
	reflect.Uint32:        true,
	reflect.Uint64:        true,
	reflect.Uintptr:       true,
	reflect.Float32:       true,
	reflect.Float64:       true,
	reflect.Complex64:     true,
	reflect.Complex128:    true,
	reflect.Array:         true,
	reflect.String:        true,
	reflect.Struct:        true,
	reflect.UnsafePointer: true,
}

type reflectFunc struct {
	name        string
	args        []argDef
	minimumArgs int
	value       reflect.Value
}

type Argument struct {
	Name     string
	Default  any
	Optional bool
}

type argDef struct {
	name                   string
	valueType              reflect.Type
	zeroValue              reflect.Value
	defaultValue           reflect.Value
	referencedValueNilable bool
}

func (d *argDef) newPointerDefaultValue() reflect.Value {
	p := reflect.New(d.defaultValue.Type())
	p.Elem().Set(d.defaultValue)
	return p
}

func (d *argDef) getDefaultValue() reflect.Value {
	if d.referencedValueNilable {
		if d.defaultValue.Type().Kind() == reflect.Ptr {
			return d.defaultValue
		}
		return d.newPointerDefaultValue()
	}

	switch d.defaultValue.Kind() {
	case reflect.Array, reflect.Map:
		return d.deepClone(d.defaultValue)

	default:
		return d.defaultValue
	}
}

func (d *argDef) deepClone(src reflect.Value) reflect.Value {
	switch src.Kind() {
	case reflect.Array:
		if src.IsNil() {
			return src
		}

		dst := reflect.MakeSlice(src.Type(), src.Len(), src.Cap())
		for i, l := 0, src.Len(); i < l; i++ {
			dst.Index(i).Set(d.deepClone(src.Index(i)))
		}
		return dst

	case reflect.Map:
		if src.IsNil() {
			return src
		}

		dst := reflect.MakeMap(src.Type())
		for iter := src.MapRange(); iter.Next(); {
			dst.SetMapIndex(reflect.ToValue(iter.Key()), d.deepClone(reflect.ToValue(iter.Value())))
		}
		return dst

	default:
		return src
	}
}

var errorInterfaceType = reflect.TypeOf((*error)(nil)).Elem()

func NewFunction(name string, args []Argument, f any) (Function, error) {
	v := reflect.ValueOf(f)
	if v.Kind() != reflect.Func {
		return nil, fmt.Errorf("must be function but got %T: %+v", f, f)
	}

	t := v.Type()
	if t.NumIn() != len(args) {
		return nil, fmt.Errorf("mis-match arguments count with args %+v: %+v", args, f)
	}
	if t.NumOut() != 2 {
		return nil, fmt.Errorf("builtin function must return 2 values: %+v", f)
	}
	if lastOut := t.Out(1); !lastOut.Implements(errorInterfaceType) {
		return nil, fmt.Errorf("last return value type must be error: %s", lastOut.String())
	}

	minimumArgs := 0
	defs := make([]argDef, len(args))
	for i, arg := range args {
		argType := t.In(i)

		// fill argDef
		defs[i].name = arg.Name
		defs[i].valueType = argType
		defs[i].zeroValue = reflect.New(argType).Elem()
		defs[i].referencedValueNilable = argType.Kind() == reflect.Ptr && nonNilableTypeSet[argType.Elem().Kind()]
		if arg.Default != nil {
			defs[i].defaultValue = reflect.ValueOf(arg.Default)
		} else if arg.Optional {
			defs[i].defaultValue = reflect.Zero(argType)
		}

		// this is required
		if arg.Default == nil && !arg.Optional {
			if i == 0 || (i == minimumArgs && !defs[i-1].defaultValue.IsValid()) {
				minimumArgs++
			}
			continue
		}

		// must not set both
		if arg.Default != nil && arg.Optional {
			return nil, fmt.Errorf("argument[%d] %s's default value is must be nil to be optional", i, arg.Name)
		}

		// check assignable
		if defs[i].referencedValueNilable {
			if !defs[i].defaultValue.Type().AssignableTo(argType.Elem()) {
				return nil, fmt.Errorf("argument[%d] %s's default value %+v(%T) is not assignable to %s", i, arg.Name, arg.Default, arg.Default, argType.Elem().String())
			}
		} else {
			if !defs[i].defaultValue.Type().AssignableTo(argType) {
				return nil, fmt.Errorf("argument[%d] %s's default value %+v(%T) is not assignable to %s", i, arg.Name, arg.Default, arg.Default, argType.String())
			}
		}
	}

	return &reflectFunc{
		name:        name,
		args:        defs,
		minimumArgs: minimumArgs,
		value:       v,
	}, nil
}

func MustNewFunction(name string, args []Argument, f any) Function {
	fun, err := NewFunction(name, args, f)
	if err != nil {
		panic(err)
	}
	return fun
}

func (f *reflectFunc) Name() string {
	return f.name
}

func (f *reflectFunc) Args() []string {
	return lo.Map(f.args, func(def argDef, _ int) string {
		return def.name
	})
}

func (f *reflectFunc) Call(args []any) (any, error) {
	if len(args) > len(f.args) {
		return nil, fmt.Errorf("too many arguments: %d arguments are allowed but got %d arguments, usage: %s(%s)", len(f.args), len(args), f.name, renderArgDefs(f.args))
	}
	if !(f.minimumArgs <= len(args) && len(args) <= len(f.args)) {
		return nil, fmt.Errorf("missing arguments: %d arguments are required but got %d arguments, usage: %s(%s)", f.minimumArgs, len(args), f.name, renderArgDefs(f.args))
	}

	argValues := make([]reflect.Value, len(f.args))
	for i, arg := range f.args {
		// fill default value for missing args
		if i >= len(args) || args[i] == SubstitutionNone {
			if !arg.defaultValue.IsValid() {
				return nil, fmt.Errorf("missing argument[%d] %s", i, arg.name)
			}
			argValues[i] = arg.getDefaultValue()
			continue
		}

		argValues[i] = reflect.ValueOf(args[i])

		// fill zero value for explicit nil value
		if !argValues[i].IsValid() {
			argValues[i] = arg.zeroValue
			continue
		}

		// check assignable
		if argValues[i].Type().AssignableTo(arg.valueType) {
			continue // OK
		}
		if arg.referencedValueNilable && argValues[i].Type().AssignableTo(arg.valueType.Elem()) {
			v := reflect.New(arg.valueType.Elem())
			v.Elem().Set(argValues[i])
			argValues[i] = v
			continue // OK
		}

		return nil, fmt.Errorf("invalid argument[%d] %s: expected type is %s but actual %s (%+v)", i, arg.name, arg.valueType.String(), argValues[i].Type().String(), argValues[i].Interface())
	}

	ret := f.value.Call(argValues)
	if !ret[1].IsZero() {
		err := ret[1].Interface().(error)
		return nil, err
	}

	result := ret[0].Interface()
	return result, nil
}

func renderArgDefs(args []argDef) string {
	var s strings.Builder
	for i, arg := range args {
		if i != 0 {
			s.WriteString(", ")
		}

		s.WriteString(arg.name)
		if !arg.defaultValue.IsValid() {
			continue
		} else if arg.defaultValue.IsZero() {
			s.WriteByte('?')
		} else {
			s.WriteString(" = ")
			fmt.Fprint(&s, arg.defaultValue.Interface())
		}
	}
	return s.String()
}

func NewRawFunction(name string, args []Argument, f func([]any) (any, error)) Function {
	return &rawFunction{
		name: name,
		args: args,
		f:    f,
	}
}

type rawFunction struct {
	name string
	args []Argument
	f    func([]any) (any, error)
}

func (f *rawFunction) Name() string {
	return f.name
}

func (f *rawFunction) Args() []string {
	return lo.Map(f.args, func(def Argument, _ int) string {
		return def.Name
	})
}

func (f *rawFunction) Call(args []any) (any, error) {
	if len(args) > len(f.args) {
		return nil, fmt.Errorf("invalid function usage: %s(%s)", f.name, renderArguments(f.args))
	}
	for i, arg := range args {
		if arg != SubstitutionNone {
			continue
		}

		if f.args[i].Optional {
			args[i] = nil
		} else if f.args[i].Default != nil {
			args[i] = f.args[i].Default
		}
	}
	for len(args) < len(f.args) {
		if f.args[len(args)].Optional {
			break
		}
		args = append(args, f.args[len(args)].Default)
	}
	return f.f(args)
}

func renderArguments(args []Argument) string {
	var s strings.Builder
	for i, arg := range args {
		if i != 0 {
			s.WriteString(", ")
		}

		s.WriteString(arg.Name)
		if arg.Optional {
			s.WriteByte('?')
		} else if arg.Default != nil {
			s.WriteString(" = ")
			fmt.Fprint(&s, arg.Default)
		}
	}
	return s.String()
}
