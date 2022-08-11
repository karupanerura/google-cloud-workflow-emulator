package expression

// ast is S expression
type ast struct {
	atom token
	list []*ast
}
