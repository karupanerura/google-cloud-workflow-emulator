package expression

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/k0kubun/pp"
	"github.com/samber/lo"
)

var prefixOperatorBindingPowerMap = map[string]uint8{
	"not": 3,
	"-":   6,
	"+":   6,
	"(":   6,
	"[":   6,
}

var infixOperatorBindingPowerMap = map[string]uint8{
	",":   0,
	"and": 1,
	"or":  1,
	"==":  2,
	"!=":  2,
	"<":   2,
	"<=":  2,
	">":   2,
	">=":  2,
	"in":  2,
	"+":   4,
	"-":   4,
	"*":   5,
	"/":   5,
	"//":  5,
	"%":   5,
	".":   7,
}

var parenthesisPairMap = map[string]string{
	"(": ")",
	"[": "]",
}

var parenthesisReversePairMap = lo.Invert(parenthesisPairMap)

var parserDebugLog = false

func init() {
	if v, err := strconv.ParseBool(os.Getenv("WORKFLOW_EMULATOR_EXPRESSION_DEBUG")); v && err == nil {
		parserDebugLog = true
	}
}

type parser struct {
	source string
	debug  bool
}

func ValueExpr[T any](value T) *Expr {
	return &Expr{
		Source:    "(value)",
		operation: &valueOperation[T]{value: value},
	}
}

func ParseExpr(source string) (*Expr, error) {
	p := &parser{source: source, debug: parserDebugLog}
	return p.parse()
}

func ParseExprWithDebugOutput(source string) (*Expr, error) {
	p := &parser{source: source, debug: true}
	return p.parse()
}

func (p *parser) parse() (*Expr, error) {
	lex := newLexer(p.source)
	sExpr, err := p.constructAST(lex, 0)
	if errors.Is(err, io.EOF) {
		// ok: ignore it
	} else if err != nil {
		return nil, err
	}
	if !lex.isCompleted() {
		tok, err := lex.consume()
		if err != nil {
			return nil, err
		}
		if p.debug {
			log.Println("not consumed token: ", p.extractLiteralString(tok))
		}
		return nil, p.createInvalidTokenError(tok)
	}
	if sExpr == nil {
		return nil, fmt.Errorf("empty expression is not allowd")
	}

	if p.debug {
		pp.Println(p.source)
		pp.Println(sExpr)
		log.Println(p.renderAST(sExpr))
	}

	op, err := p.constructOperation(sExpr)
	if err != nil {
		return nil, err
	}

	return &Expr{
		Source:    p.source,
		operation: op,
	}, nil
}

func (p *parser) constructAST(lex *lexer, minBP uint8) (*ast, error) {
	tok, err := lex.consume()
	if err != nil {
		return nil, err
	}
	if p.debug {
		log.Println("first token: ", p.extractLiteralString(tok))
	}

	left := &ast{atom: tok}
	if _, isOP := tok.(operatorToken); isOP {
		op := p.extractLiteralString(tok)
		if bp, isPrefixOP := prefixOperatorBindingPowerMap[op]; isPrefixOP {
			if closeOP, isLeftParen := parenthesisPairMap[op]; isLeftParen {
				sExpr, err := p.constructAST(lex, 0)
				if errors.Is(err, io.EOF) {
					return nil, p.createInvalidTokenError(tok)
				} else if err != nil {
					return nil, err
				}

				nextTok, err := lex.consume()
				if errors.Is(err, io.EOF) {
					return nil, p.createInvalidTokenError(tok)
				} else if err != nil {
					return nil, err
				}
				if p.debug {
					log.Println("next of paren token: ", p.extractLiteralString(nextTok))
				}

				if _, isOp := nextTok.(operatorToken); !isOp {
					return nil, p.createInvalidTokenError(nextTok)
				} else if p.extractLiteralString(nextTok) != closeOP {
					return nil, p.createInvalidTokenError(nextTok)
				}

				left = &ast{list: []*ast{{atom: tok}, sExpr}}
			} else {
				sExpr, err := p.constructAST(lex, bp+1)
				if errors.Is(err, io.EOF) {
					// ok: ignore it
				} else if err != nil {
					return nil, err
				}
				if sExpr == nil {
					return nil, p.createInvalidTokenError(tok)
				}
				if sExpr.list != nil && len(sExpr.list) == 2 {
					if opTok, isOP := sExpr.list[0].atom.(operatorToken); isOP {
						if op := p.extractLiteralString(opTok); op == "+" || op == "-" {
							return nil, p.createInvalidTokenError(opTok)
						}
					}
				}

				left = &ast{list: []*ast{{atom: tok}, sExpr}}
			}
		} else {
			return nil, p.createInvalidTokenError(tok)
		}
	}

	for {
		tok, err := lex.consume()
		if errors.Is(err, io.EOF) {
			return left, nil
		} else if err != nil {
			return nil, err
		}

		if _, isOP := tok.(operatorToken); isOP {
			op := p.extractLiteralString(tok)
			if p.debug {
				log.Println("OP", minBP, op, p.renderAST(left))
			}
			if bp, isInfixOP := infixOperatorBindingPowerMap[op]; isInfixOP {
				if bp < minBP {
					lex.push(tok)
					return left, nil
				}
				if p.debug {
					log.Println("third op token: ", p.extractLiteralString(tok))
				}

				sExpr, err := p.constructAST(lex, bp+1)
				if errors.Is(err, io.EOF) {
					// ok: ignore it
				} else if err != nil {
					return nil, err
				}
				if sExpr == nil {
					return nil, p.createInvalidTokenError(tok)
				}

				left = &ast{list: []*ast{{atom: tok}, left, sExpr}}
				continue
			} else if closeOP, isLeftParen := parenthesisPairMap[op]; isLeftParen {
				bp := prefixOperatorBindingPowerMap[op]
				if bp < minBP {
					lex.push(tok)
					return left, nil
				}

				if p.debug {
					log.Println("third paren token: ", p.extractLiteralString(tok))
				}
				if closeOP == ")" {
					nextTok, err := lex.consume()
					if errors.Is(err, io.EOF) {
						return nil, p.createInvalidTokenError(tok)
					} else if err != nil {
						return nil, err
					}

					if _, isOp := nextTok.(operatorToken); isOp && p.extractLiteralString(nextTok) == closeOP {
						if p.debug {
							log.Println("close op token: ", p.extractLiteralString(nextTok))
						}
						left = &ast{list: []*ast{{atom: tok}, left, nil}}
						continue
					}
					lex.push(nextTok)
				}

				sExpr, err := p.constructAST(lex, 0)
				if errors.Is(err, io.EOF) {
					return nil, p.createInvalidTokenError(tok)
				} else if err != nil {
					return nil, err
				}

				nextTok, err := lex.consume()
				if errors.Is(err, io.EOF) {
					return nil, p.createInvalidTokenError(tok)
				} else if err != nil {
					return nil, err
				}
				if p.debug {
					log.Println("third paren closing: ", p.extractLiteralString(nextTok))
				}

				if _, isOp := nextTok.(operatorToken); !isOp {
					return nil, p.createInvalidTokenError(nextTok)
				} else if p.extractLiteralString(nextTok) != closeOP {
					return nil, p.createInvalidTokenError(nextTok)
				}

				left = &ast{list: []*ast{{atom: tok}, left, sExpr}}
				continue
			} else {
				lex.push(tok)
				return left, nil
			}
		} else {
			if p.debug {
				log.Println("token: ", p.extractLiteralString(tok))
			}
			return nil, p.createInvalidTokenError(tok)
		}
	}
}

func (p *parser) constructOperation(sExpr *ast) (operation, error) {
	if sExpr.list == nil {
		return p.constructOperationByAtom(sExpr.atom)
	}

	switch len(sExpr.list) {
	case 2:
		first := sExpr.list[0]
		if first.list != nil {
			panic(fmt.Sprintf("invalid AST: %s", p.renderAST(sExpr)))
		}

		opTok, isOP := first.atom.(operatorToken)
		if !isOP {
			panic(fmt.Sprintf("invalid AST: %s", p.renderAST(sExpr)))
		}

		switch op := p.extractLiteralString(opTok); op {
		case "(":
			return p.constructOperation(sExpr.list[1])

		default:
			ope, err := p.constructOperation(sExpr.list[1])
			if err != nil {
				return nil, err
			}

			return &calculateUnaryOperation{
				operator: op,
				value:    ope,
			}, nil
		}

	case 3:
		first := sExpr.list[0]
		if first.list != nil {
			panic(fmt.Sprintf("invalid AST: %s", p.renderAST(sExpr)))
		}

		opTok, isOP := first.atom.(operatorToken)
		if !isOP {
			panic(fmt.Sprintf("invalid AST: %s", p.renderAST(sExpr)))
		}

		switch op := p.extractLiteralString(opTok); op {
		case "[":
			leftOpe, err := p.constructOperation(sExpr.list[1])
			if err != nil {
				return nil, err
			}

			rightOpe, err := p.constructOperation(sExpr.list[2])
			if err != nil {
				return nil, err
			}

			return &retrieveFieldOperation{
				context: leftOpe,
				field:   rightOpe,
			}, nil

		case ".":
			contextOpe, err := p.constructOperation(sExpr.list[1])
			if err != nil {
				return nil, err
			}

			third := sExpr.list[2]
			if third.list != nil {
				return nil, p.createInvalidTokenError(opTok)
			}

			symTok, isSym := third.atom.(symbolLiteralToken)
			if !isSym {
				return nil, p.createInvalidTokenError(third.atom)
			}

			return &retrieveFieldOperation{
				context: contextOpe,
				field:   &stringLiteralOperation{value: p.extractLiteralString(symTok)},
			}, nil

		case "(": // function call
			functionOpe, err := p.constructOperation(sExpr.list[1])
			if err != nil {
				return nil, err
			}

			var args []operation
			if sExpr.list[2] != nil { // nil means no arguments
				ope, err := p.constructOperation(sExpr.list[2])
				if err != nil {
					return nil, err
				}
				args = p.expandComma(ope)
			}

			return &callFunctionOperation{
				function: functionOpe,
				args:     args,
			}, nil

		default:
			leftOpe, err := p.constructOperation(sExpr.list[1])
			if err != nil {
				return nil, err
			}

			rightOpe, err := p.constructOperation(sExpr.list[2])
			if err != nil {
				return nil, err
			}

			return &calculateBinaryOperation{
				operator: op,
				left:     leftOpe,
				right:    rightOpe,
			}, nil
		}

	default:
		panic(fmt.Sprintf("invalid AST: %s", p.renderAST(sExpr)))
	}
}

func (p *parser) expandComma(ope operation) []operation {
	if o, isOP := ope.(*calculateBinaryOperation); isOP && o.operator == "," {
		left := p.expandComma(o.left)
		right := p.expandComma(o.right)
		return append(left, right...)
	}
	return []operation{ope}
}

func (p *parser) constructOperationByAtom(t token) (operation, error) {
	switch t.(type) {
	case booleanLiteralToken:
		v, err := strconv.ParseBool(p.extractLiteralString(t))
		if err != nil {
			panic(fmt.Sprintf("invalid boolean %s at %d: %v", p.extractLiteralString(t), t.BeginsPos(), err))
		}

		return &booleanLiteralOperation{value: v}, nil

	case nullLiteralToken:
		return nullLiteralOperation, nil

	case stringLiteralToken:
		return &stringLiteralOperation{value: p.getContentByStringToken(t)}, nil

	case numericLiteralToken:
		if v := p.extractLiteralString(t); strings.IndexByte(v, '.') == -1 {
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer %s at %d: %w", v, t.BeginsPos(), err)
			}

			return &int64LiteralOperation{value: vv}, nil
		} else {
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number %s at %d: %w", v, t.BeginsPos(), err)
			}

			return &float64LiteralOperation{value: vv}, nil
		}

	case symbolLiteralToken:
		return &retrieveSymbolOperation{name: p.extractLiteralString(t)}, nil

	default:
		return nil, p.createInvalidTokenError(t)
	}
}

func (p *parser) extractLiteralString(t token) string {
	return p.source[t.BeginsPos():t.EndsPos()]
}

func (p *parser) createInvalidTokenError(t token) error {
	return fmt.Errorf("invalid token %s at %d: expr=%q", p.extractLiteralString(t), t.BeginsPos()+1, p.source)
}

var stringLiteralEscapeReplacer = strings.NewReplacer(
	"\\n", "\n",
	"\\\"", `"`,
	"\\r", "\r",
	"\\t", "\t",
	"\\\\", "\\",
	"\\}", "}",
)

func (p *parser) getContentByStringToken(t token) string {
	s := p.extractLiteralString(t)
	return stringLiteralEscapeReplacer.Replace(s[1 : len(s)-1])
}

func (p *parser) renderAST(sExpr *ast) string {
	if sExpr == nil {
		return "nil"
	}
	if sExpr.list != nil {
		var b strings.Builder
		b.WriteByte('(')
		for i, expr := range sExpr.list {
			if i != 0 {
				b.WriteByte(' ')
			}
			b.WriteString(p.renderAST(expr))
		}
		b.WriteByte(')')
		return b.String()
	}

	switch sExpr.atom.(type) {
	case nullLiteralToken, booleanLiteralToken, symbolLiteralToken, numericLiteralToken:
		return p.source[sExpr.atom.BeginsPos():sExpr.atom.EndsPos()]
	default:
		return strconv.Quote(p.source[sExpr.atom.BeginsPos():sExpr.atom.EndsPos()])
	}
}
