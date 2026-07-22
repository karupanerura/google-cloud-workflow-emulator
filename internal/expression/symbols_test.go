package expression_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/expression"
)

func TestSymbolName(t *testing.T) {
	expr, err := expression.ParseExpr("foo")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	if name, ok := expr.SymbolName(); !ok || name != "foo" {
		t.Errorf("should be symbol foo but got %q (ok=%v)", name, ok)
	}

	expr, err = expression.ParseExpr("foo.bar")
	if err != nil {
		t.Fatalf("ParseExpr: %v", err)
	}
	if name, ok := expr.SymbolName(); ok {
		t.Errorf("should not be a plain symbol but got %q", name)
	}
}

func TestAppendReferencedSymbols(t *testing.T) {
	for _, tt := range []struct {
		source   string
		expected []string
	}{
		{source: "foo", expected: []string{"foo"}},
		{source: "foo.bar", expected: []string{"foo"}},
		{source: "foo[i].bar", expected: []string{"foo", "i"}},
		{source: "a + b * -c", expected: []string{"a", "b", "c"}},
		{source: `f(x, "literal", 1) + y.z`, expected: []string{"f", "x", "y"}},
		{source: `"literal"`, expected: []string{}},
	} {
		tt := tt
		t.Run(tt.source, func(t *testing.T) {
			expr, err := expression.ParseExpr(tt.source)
			if err != nil {
				t.Fatalf("ParseExpr: %v", err)
			}

			set := map[string]struct{}{}
			expr.AppendReferencedSymbols(set)

			got := make([]string, 0, len(set))
			for name := range set {
				got = append(got, name)
			}
			sort.Strings(got)

			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("should collect %v but got %v", tt.expected, got)
			}
		})
	}
}
