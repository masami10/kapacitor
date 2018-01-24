package tick

import (
	"github.com/masami10/kapacitor/pipeline"
	"github.com/masami10/kapacitor/tick/ast"
)

// UnionNode converts the union pipeline node into the TICKScript AST
type UnionNode struct {
	Function
}

// NewUnion creates a Union function builder
func NewUnion(parents []ast.Node) *UnionNode {
	return &UnionNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a union ast.Node
func (n *UnionNode) Build(u *pipeline.UnionNode) (ast.Node, error) {
	unioned := []interface{}{}
	for _, p := range n.Parents[1:] {
		unioned = append(unioned, p)
	}
	n.Pipe("union", unioned...).
		Dot("rename", u.Rename)
	return n.prev, n.err
}
