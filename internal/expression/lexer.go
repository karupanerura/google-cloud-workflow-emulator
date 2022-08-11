package expression

import (
	"fmt"
	"io"
)

type lexer struct {
	source    string
	index     int
	lastIndex int
	stack     []lexerContext
	buf       []token
}

func newLexer(source string) *lexer {
	return &lexer{
		source:    source,
		index:     0,
		lastIndex: len(source) - 1,
		stack: []lexerContext{
			{kind: defaultLexerContext},
		},
		buf: nil,
	}
}

type lexerContextKind int

const (
	defaultLexerContext lexerContextKind = iota
	stringLiteralLexerContext
	numericLiteralLexerContext
	symbolLiteralLexerContext
)

type lexerContext struct {
	kind           lexerContextKind
	rangeBeginsIdx int
	dotFound       bool
}

func (l *lexer) isCompleted() bool {
	return l.index == len(l.source) && len(l.buf) == 0
}

func (l *lexer) push(t token) {
	l.buf = append(l.buf, t)
}

func (l *lexer) consume() (token, error) {
	if len(l.stack) == 0 {
		panic(fmt.Sprintf("should not leach here: source=%s", l.source))
	}
	if len(l.buf) != 0 {
		tok := l.buf[len(l.buf)-1]
		l.buf = l.buf[:len(l.buf)-1]
		return tok, nil
	}

	for l.index != len(l.source) {
		context := l.stack[len(l.stack)-1]
		switch context.kind {
		case defaultLexerContext:
			switch l.source[l.index] {
			case ' ', '\t', '\n':
				l.index++ // just skip white spaces
			case '"':
				l.stack = append(l.stack, lexerContext{kind: stringLiteralLexerContext, rangeBeginsIdx: l.index})
				l.index++
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				if l.index == l.lastIndex {
					l.index++
					return numericLiteralToken{rangeToken{beginsPos: l.index - 1, endsPos: l.index}}, nil
				} else {
					l.stack = append(l.stack, lexerContext{kind: numericLiteralLexerContext, rangeBeginsIdx: l.index})
					l.index++
				}
			case '/':
				if l.index != l.lastIndex && l.source[l.index+1] == '/' {
					l.index += 2
					return operatorToken{rangeToken{beginsPos: l.index - 2, endsPos: l.index}}, nil
				} else {
					l.index++
					return operatorToken{rangeToken{beginsPos: l.index - 1, endsPos: l.index}}, nil
				}
			case '+', '-', '*', '.', '%', ',', '(', ')', '[', ']':
				l.index++
				return operatorToken{rangeToken{beginsPos: l.index - 1, endsPos: l.index}}, nil
			case '<', '>':
				if l.index != l.lastIndex && l.source[l.index+1] == '=' {
					l.index += 2
					return operatorToken{rangeToken{beginsPos: l.index - 2, endsPos: l.index}}, nil
				} else {
					l.index++
					return operatorToken{rangeToken{beginsPos: l.index - 1, endsPos: l.index}}, nil
				}
			case '=', '!':
				if l.index != l.lastIndex && l.source[l.index+1] == '=' {
					l.index += 2
					return operatorToken{rangeToken{beginsPos: l.index - 2, endsPos: l.index}}, nil
				} else {
					return nil, fmt.Errorf("invalid charactor at %d: %c", l.index, l.source[l.index])
				}
			default:
				if c := l.source[l.index]; ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_' {
					if l.index == l.lastIndex {
						l.index++
						return symbolLiteralToken{rangeToken{beginsPos: l.index - 1, endsPos: l.index}}, nil
					}
					l.stack = append(l.stack, lexerContext{kind: symbolLiteralLexerContext, rangeBeginsIdx: l.index})
					l.index++
				} else {
					return nil, fmt.Errorf("invalid charactor at %d: %c", l.index, l.source[l.index])
				}
			}

		case stringLiteralLexerContext:
			switch l.source[l.index] {
			case '"':
				l.index++
				l.stack = l.stack[:len(l.stack)-1]
				return stringLiteralToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil
			case '\\':
				if l.index != l.lastIndex && l.source[l.index+1] == '"' {
					if l.index >= l.lastIndex-1 {
						return nil, fmt.Errorf("operation error unbaranced literal in source: %s", l.source)
					}
					l.index += 2
				} else {
					if l.index == l.lastIndex {
						return nil, fmt.Errorf("operation error unbaranced literal in source: %s", l.source)
					}
					l.index++
				}
			default:
				if l.index == l.lastIndex {
					return nil, fmt.Errorf("operation error unbaranced literal in source: %s", l.source)
				}
				l.index++
			}

		case numericLiteralLexerContext:
			if l.index == l.lastIndex {
				if c := l.source[l.index]; c == '.' {
					return nil, fmt.Errorf("invalid charactor at %d: %c", l.index, c)
				} else if '0' <= c && c <= '9' {
					l.index++
				}
			} else {
				if c := l.source[l.index]; c == '.' {
					if context.dotFound {
						return nil, fmt.Errorf("invalid charactor at %d: %c", l.index, c)
					}
					context.dotFound = true
					l.index++
					continue
				} else if '0' <= c && c <= '9' {
					l.index++
					continue
				}
			}
			if l.source[l.index-1] == '.' {
				l.index--
			}

			l.stack = l.stack[:len(l.stack)-1]
			return numericLiteralToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil

		case symbolLiteralLexerContext:
			if c := l.source[l.index]; ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '_' {
				l.index++
				if l.index != l.lastIndex+1 {
					continue
				}
			}

			l.stack = l.stack[:len(l.stack)-1]
			switch l.source[context.rangeBeginsIdx:l.index] {
			case "and", "or", "not", "in":
				return operatorToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil
			case "true", "True", "TRUE", "false", "False", "FALSE":
				return booleanLiteralToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil
			case "null":
				return nullLiteralToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil
			default:
				return symbolLiteralToken{rangeToken{beginsPos: context.rangeBeginsIdx, endsPos: l.index}}, nil
			}
		}
	}

	switch len(l.stack) {
	case 0:
		panic(fmt.Sprintf("should not leach here: source=%s", l.source))
	case 1:
		if l.stack[0].kind != defaultLexerContext {
			panic(fmt.Sprintf("should not leach here: source=%s", l.source))
		}
		return nil, io.EOF
	default:
		return nil, fmt.Errorf("operation error unbaranced literal in source: %s", l.source)
	}
}
