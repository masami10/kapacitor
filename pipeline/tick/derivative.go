package tick

import (
	"github.com/masami10/kapacitor/pipeline"
	"github.com/masami10/kapacitor/tick/ast"
)

// DerivativeNode converts the Derivative pipeline node into the TICKScript AST
type DerivativeNode struct {
	Function
}

// NewDerivative creates a Derivative function builder
func NewDerivative(parents []ast.Node) *DerivativeNode {
	return &DerivativeNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Derivative ast.Node
func (n *DerivativeNode) Build(d *pipeline.DerivativeNode) (ast.Node, error) {
	n.Pipe("derivative", d.Field).
		Dot("as", d.As).
		Dot("unit", d.Unit).
		DotIf("nonNegative", d.NonNegativeFlag)
	return n.prev, n.err
}
