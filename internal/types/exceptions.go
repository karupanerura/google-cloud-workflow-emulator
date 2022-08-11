package types

import (
	"errors"
	"fmt"
	"strings"

	"github.com/samber/lo"
)

type ErrorTag string

const (
	AuthErrorTag            ErrorTag = "AuthError"
	ConnectionErrorTag      ErrorTag = "ConnectionError"
	HttpErrorTag            ErrorTag = "HttpError"
	IndexErrorTag           ErrorTag = "IndexError"
	KeyErrorTag             ErrorTag = "KeyError"
	ParallelNestingErrorTag ErrorTag = "ParallelNestingError"
	RecursionErrorTag       ErrorTag = "RecursionError"
	ResourceLimitErrorTag   ErrorTag = "ResourceLimitError"
	SystemErrorTag          ErrorTag = "SystemError"
	TypeErrorTag            ErrorTag = "TypeError"
	UnhandledBranchErrorTag ErrorTag = "UnhandledBranchError"
	ValueErrorTag           ErrorTag = "ValueError"
	ZeroDivisionErrorTag    ErrorTag = "ZeroDivisionError"
)

type Exception interface {
	error
	Exception() any
}

type stringException string

func (s stringException) Error() string {
	return string(s)
}

func (s stringException) Exception() any {
	return string(s)
}

func NewExceptionByString(s string) Exception {
	return stringException(s)
}

type mapException map[string]any

func (m mapException) Error() string {
	return fmt.Sprintf("custom map exception: %+v", m.Exception())
}

func (m mapException) Exception() any {
	return (map[string]any)(m)
}

func NewExceptionByMap(m map[string]any) Exception {
	return (mapException)(m)
}

type Error struct {
	Tag   ErrorTag
	Err   error
	Extra map[string]any
}

var _ Exception = (*Error)(nil)

func (e *Error) Error() string {
	if e.Err == nil {
		return string(e.Tag)
	}

	var b strings.Builder
	b.WriteString(string(e.Tag))
	b.WriteString(": ")
	b.WriteString(e.Err.Error())
	return b.String()
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) Exception() any {
	tags := []any{e.Tag}
	for err := error(e); err != nil; err = errors.Unwrap(err) {
		if e, ok := err.(*Error); ok {
			tags = append(tags, e.Tag)
		}
	}

	o := map[string]any{
		"tags": tags,
	}
	if len(e.Extra) != 0 {
		o = lo.Assign(o, e.Extra)
	}
	return o
}
