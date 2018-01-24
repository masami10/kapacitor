package tick

import (
	"github.com/masami10/kapacitor/pipeline"
	"github.com/masami10/kapacitor/tick/ast"
)

// FlattenNode converts the FlattenNode pipeline node into the TICKScript AST
type FlattenNode struct {
	Function
}

// NewFlatten creates a FlattenNode function builder
func NewFlatten(parents []ast.Node) *FlattenNode {
	return &FlattenNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Flatten ast.Node
func (n *FlattenNode) Build(f *pipeline.FlattenNode) (ast.Node, error) {
	n.Pipe("flatten").
		Dot("on", args(f.Dimensions)...).
		Dot("delimiter", f.Delimiter).
		Dot("tolerance", f.Tolerance).
		DotIf("dropOriginalFieldName", f.DropOriginalFieldNameFlag)

	return n.prev, n.err
}
