package expression_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/expression"
	"github.com/karupanerura/google-cloud-workflow-emulator/internal/types"
)

type function func([]any) (any, error)

func (f function) Call(args []any) (any, error) {
	return f(args)
}

func TestParseExpr(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		symbols               *types.SymbolTable
		source                string
		expected              any
		expectToBeParseErr    bool
		expectToBeEvaluateErr bool
		debug                 bool
	}{
		{
			source:             "+",
			expectToBeParseErr: true,
		},
		{
			source:             "-",
			expectToBeParseErr: true,
		},
		{
			source:             "*",
			expectToBeParseErr: true,
		},
		{
			source:             "/",
			expectToBeParseErr: true,
		},
		{
			source:             "//",
			expectToBeParseErr: true,
		},
		{
			source:             "i++",
			expectToBeParseErr: true,
		},
		{
			source:             "++i",
			expectToBeParseErr: true,
		},
		{
			source:             "()",
			expectToBeParseErr: true,
		},
		{
			source:             "((1)",
			expectToBeParseErr: true,
		},
		{
			source:             "(1))",
			expectToBeParseErr: true,
		},
		{
			source:             "f((1)",
			expectToBeParseErr: true,
		},
		{
			source:             "f(1))",
			expectToBeParseErr: true,
		},
		{
			source:             "s[[1]",
			expectToBeParseErr: true,
		},
		{
			source:             "f[1]]",
			expectToBeParseErr: true,
		},
		{
			source:   "true",
			expected: true,
		},
		{
			source:   "false",
			expected: false,
		},
		{
			source:   "null",
			expected: nil,
		},
		{
			source:   "(true)",
			expected: true,
		},
		{
			source:   "((false))",
			expected: false,
		},
		{
			source: "sym",
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]string{"a": "b"},
				},
			},
			expected: map[string]string{"a": "b"},
		},
		{
			source: "unknown",
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]string{"a": "b"},
				},
			},
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": true,
					},
				},
			},
			source:   "sym.foo",
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": true,
					},
				},
			},
			source:   `sym["foo"]`,
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": []any{false, true},
				},
			},
			source:   `sym[1]`,
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"f+o+o": []any{true},
					},
				},
			},
			source:   `sym["f+o+o"][0]`,
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"f+o+o": []any{true},
					},
				},
			},
			source:                `sym[0]`,
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"f+o+o": []any{true},
					},
				},
			},
			source:                `sym["f+o+o"][1]`,
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"f+o+o": []any{true},
					},
				},
			},
			source:                `sym["f+o+o"][-1]`,
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"f+o+o": []any{true},
					},
				},
			},
			source:                `sym["f+o+o"]["0"]`,
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": map[string]any{
							"bar": false,
						},
					},
				},
			},
			source:   "sym.foo.bar",
			expected: false,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": map[string]any{
							"bar": map[string]any{},
						},
					},
				},
			},
			source:                "sym.foo.bar.unknown",
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": 1,
				},
			},
			source:                "sym.foo",
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": function(func([]any) (any, error) {
						return "called!", nil
					}),
				},
			},
			source:   `sym()`,
			expected: "called!",
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": function(func([]any) (any, error) {
							return "called!!", nil
						}),
					},
				},
			},
			source:   "sym.foo()",
			expected: "called!!",
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": map[string]any{
						"foo": map[string]any{
							"bar": function(func([]any) (any, error) {
								return "called!!!", nil
							}),
						},
					},
				},
			},
			source:   "sym.foo.bar()",
			expected: "called!!!",
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": nil,
				},
			},
			source:                "sym()",
			expectToBeEvaluateErr: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": function(func([]any) (any, error) {
						return nil, fmt.Errorf("function internal error")
					}),
				},
			},
			source:                `sym()`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "123",
			expected: int64(123),
		},
		{
			source:   "-456",
			expected: int64(-456),
		},
		{
			source:   "+789",
			expected: int64(789),
		},
		{
			source:                `+"1"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "0.5",
			expected: float64(0.5),
		},
		{
			source:   "-0.5",
			expected: float64(-0.5),
		},
		{
			source:             "-0.-5",
			expectToBeParseErr: true,
		},
		{
			source:             "0.5.0",
			expectToBeParseErr: true,
		},
		{
			source:                `-"1"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "not true",
			expected: false,
		},
		{
			source:   "not(false)",
			expected: true,
		},
		{
			source:                `not 1`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   `""`,
			expected: "",
		},
		{
			source:   `"\""`,
			expected: `"`,
		},
		{
			source:   "1+2",
			expected: int64(3),
		},
		{
			source:   "1 + 2",
			expected: int64(3),
		},
		{
			source:                `"4"+5`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `4+"5"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "1-2",
			expected: int64(-1),
		},
		{
			source:   "1--1",
			expected: int64(2),
		},
		{
			source:   "-1--1",
			expected: int64(0),
		},
		{
			source:   "+1-+1",
			expected: int64(0),
		},
		{
			source:                `"1"-2`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `1-"2"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"1"-"2"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "2*3",
			expected: int64(6),
		},
		{
			source:                `2*"3"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"2"*3`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"2"*"3"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "2/4",
			expected: float64(2) / float64(4),
		},
		{
			source:   "2.0/4",
			expected: float64(2) / float64(4),
		},
		{
			source:   "2/4.0",
			expected: float64(2) / float64(4),
		},
		{
			source:   "2.0/4.0",
			expected: float64(2) / float64(4),
		},
		{
			source:                `"2.0"/4.0`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `2.0/"4.0"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"2.0"/"4.0"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "12//3",
			expected: int64(4),
		},
		{
			source:   "31%3",
			expected: int64(1),
		},
		{
			source:   "3==3",
			expected: true,
		},
		{
			source:   "3==4",
			expected: false,
		},
		{
			source:   `"foo"=="foo"`,
			expected: true,
		},
		{
			source:   `"foo"=="bar"`,
			expected: false,
		},
		{
			source:   "3==null",
			expected: false,
		},
		{
			source:   "null==3",
			expected: false,
		},
		{
			source:   `"foo"==null`,
			expected: false,
		},
		{
			source:   `null=="foo"`,
			expected: false,
		},
		{
			source:   "null==null",
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"x": function(func([]any) (any, error) {
						return nil, nil
					}),
				},
			},
			source:   "x()==null",
			expected: true,
		},
		{
			source:   "3!=3",
			expected: false,
		},
		{
			source:   "3!=4",
			expected: true,
		},
		{
			source:   "3!=2",
			expected: true,
		},
		{
			source:   "3>2",
			expected: true,
		},
		{
			source:   "3>3",
			expected: false,
		},
		{
			source:   "3>4",
			expected: false,
		},
		{
			source:   "3>=2",
			expected: true,
		},
		{
			source:   "3>=3",
			expected: true,
		},
		{
			source:   "3>=4",
			expected: false,
		},
		{
			source:   "3<2",
			expected: false,
		},
		{
			source:   "3<3",
			expected: false,
		},
		{
			source:   "3<4",
			expected: true,
		},
		{
			source:   "3<=2",
			expected: false,
		},
		{
			source:   "3<=3",
			expected: true,
		},
		{
			source:   "3<=4",
			expected: true,
		},
		{
			source:                `3<="4"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"4"<=5`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"4"<="5"`,
			expectToBeEvaluateErr: true,
		},
		{
			source:   "true and true",
			expected: true,
		},
		{
			source:   "true and false",
			expected: false,
		},
		{
			source:                "1 and true",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "false and 1",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "1.0 and true",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "false and 1.0",
			expectToBeEvaluateErr: true,
		},
		{
			source:   "true or false",
			expected: true,
		},
		{
			source:   "false or false",
			expected: false,
		},
		{
			source:                "1 or true",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "false or 1",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "1.0 or true",
			expectToBeEvaluateErr: true,
		},
		{
			source:                "false or 1.0",
			expectToBeEvaluateErr: true,
		},
		{
			source:                `"" or true`,
			expectToBeEvaluateErr: true,
		},
		{
			source:                `false or "true"`,
			expectToBeEvaluateErr: true,
		},
		{
			source: `"b" in sym`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": []any{"a", "b", "c"},
				},
			},
			expected: true,
		},
		{
			source: `"d" in sym`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": []any{"a", "b", "c"},
				},
			},
			expected: false,
		},
		{
			source: `1 in sym`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"sym": []any{"a", "b", "c"},
				},
			},
			expected: false,
		},
		{
			source: `"b" in map`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"map": map[string]any{
						"a": nil,
						"b": nil,
						"c": nil,
					},
				},
			},
			expected: true,
		},
		{
			source: `"d" in map`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"map": map[string]any{
						"a": nil,
						"b": nil,
						"c": nil,
					},
				},
			},
			expected: false,
		},
		{
			source: `1 in map`,
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"map": map[string]any{
						"a": nil,
						"b": nil,
						"c": nil,
					},
				},
			},
			expectToBeEvaluateErr: true,
		},
		{
			source:   "1+2-3*4//5",
			expected: int64(1),
		},
		{
			source:   "1+2-3*4/5",
			expected: 3.0 - 12.0/5.0,
		},
		{
			source:   `"abc"+"de"+"f"+""`,
			expected: "abcdef",
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"a": function(func(args []any) (any, error) {
						if len(args) != 1 {
							return nil, fmt.Errorf("needs a 1 argument but got: %d", len(args))
						}

						s, ok := args[0].(string)
						if !ok {
							return nil, fmt.Errorf("needs a string argument but got: %v", args[0])
						}

						return "(" + s + ")", nil
					}),
				},
			},
			source:   `a("a"+"b")+"c"`,
			expected: "(ab)c",
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"a": map[string]any{
						"b": function(func([]any) (any, error) {
							return true, nil
						}),
					},
				},
			},
			source:   `a.b()`,
			expected: true,
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"a": map[string]any{
						"b": function(func(args []any) (any, error) {
							if len(args) != 2 {
								return nil, fmt.Errorf("needs a 2 argument but got: %d", len(args))
							}

							var s int64 = 0
							for _, arg := range args {
								n, ok := arg.(int64)
								if !ok {
									return nil, fmt.Errorf("needs int64 argument but got: %v", arg)
								}
								s += n
							}
							return s, nil
						}),
					},
				},
			},
			source:   `a.b(1, a.b(2, 3)) * 3`,
			expected: int64(18),
		},
		{
			symbols: &types.SymbolTable{
				Symbols: map[string]any{
					"v": map[string]any{
						"z": int64(0),
					},
					"a": map[string]any{
						"b": function(func(args []any) (any, error) {
							if len(args) != 2 {
								return nil, fmt.Errorf("needs a 2 argument but got: %d", len(args))
							}

							var s int64 = 0
							for _, arg := range args {
								n, ok := arg.(int64)
								if !ok {
									return nil, fmt.Errorf("needs int64 argument but got: %v", arg)
								}
								s += n
							}
							return s, nil
						}),
					},
				},
			},
			source:   `a.b(1, a.b(a.b(v.z, 2), a.b(3, v.z))) * 3`,
			expected: int64(18),
		},
	} {
		tt := tt
		t.Run(tt.source, func(t *testing.T) {
			t.Parallel()

			if !tt.debug {
				// DEBUG ONLY
				// t.Skip()
			}

			parseExpr := expression.ParseExpr
			if tt.debug {
				parseExpr = expression.ParseExprWithDebugOutput
			}

			expr, err := parseExpr(tt.source)
			if err != nil {
				if tt.expectToBeParseErr {
					t.Logf("expected parse error: %v", err)
					return
				}
				t.Fatal(err)
			}
			if tt.expectToBeParseErr {
				t.Error("should be parse error")
				return
			}

			e := expression.Evaluator{SymbolTable: tt.symbols}
			ret, err := e.EvaluateValue(expr)
			if err != nil {
				if tt.expectToBeEvaluateErr {
					t.Logf("expected evaluate error: %v", err)
					return // ok
				}
				t.Fatal(err)
			}
			if tt.expectToBeEvaluateErr {
				t.Error("should be evaluate error")
				return
			}

			// check type
			if ret != nil {
				retType := reflect.TypeOf(ret)
				expectedType := reflect.TypeOf(tt.expected)
				if retType.Kind() != expectedType.Kind() {
					t.Fatalf("expect to %s but got %s (%+v)", expectedType.String(), retType.String(), ret)
				}
			}

			var isSame bool
			switch v := ret.(type) {
			case string, int64:
				isSame = v == tt.expected
			case float64:
				expected := tt.expected.(float64)
				isSame = math.Abs(v-expected) < 0.0000001
			default:
				isSame = cmp.Equal(ret, tt.expected)
			}

			if !isSame {
				t.Errorf("expect to %v but got %v", tt.expected, ret)
			}
		})
	}
}

func FuzzParseExpr(f *testing.F) {
	f.Fuzz(func(t *testing.T, source string) {
		_, err := expression.ParseExpr(source)
		if err != nil {
			t.Logf("INVALID: %q (%v)", source, err)
			return
		}

		t.Logf("PASS: %q", source)
	})
}
