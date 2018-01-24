package tick

import "github.com/masami10/kapacitor/tick/ast"

// BatchNode converts the batch pipeline node into the TICKScript AST
type BatchNode struct{}

// Build batch ast.Node
func (b *BatchNode) Build() (ast.Node, error) {
	return &ast.IdentifierNode{
		Ident: "batch",
	}, nil
}
