package postgrest

type Node struct {
	Type     pegRule
	Value    string
	Parent   *Node
	Children []*Node
}

func (n *Node) TypeString() string {
	return rul3s[n.Type]
}

func PopulateAST(expression string, parent *Node, ast *node32) {
	for {
		if ast == nil {
			break
		}

		child := &Node{}
		child.Type = ast.pegRule
		child.Value = expression[ast.begin:ast.end]

		// Remove when printing
		child.Parent = parent

		parent.Children = append(parent.Children, child)

		PopulateAST(expression, child, ast.up)

		ast = ast.next
	}
}
