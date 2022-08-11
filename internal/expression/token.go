package expression

type tokenKind int

type token interface {
	BeginsPos() int
	EndsPos() int
}

type rangeToken struct {
	beginsPos, endsPos int
}

func (t rangeToken) BeginsPos() int {
	return t.beginsPos
}

func (t rangeToken) EndsPos() int {
	return t.endsPos
}

type booleanLiteralToken struct {
	rangeToken
}

type numericLiteralToken struct {
	rangeToken
}

type stringLiteralToken struct {
	rangeToken
}

type nullLiteralToken struct {
	rangeToken
}

type symbolLiteralToken struct {
	rangeToken
}

type operatorToken struct {
	rangeToken
}
