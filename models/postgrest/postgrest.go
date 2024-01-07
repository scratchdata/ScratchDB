package postgrest

import "strconv"

type Postgrest struct {
	Filters []*Filter
	Limit   int
	Offset  int
	Order   []*Order
	// LogicalQuery
}

func (p *Postgrest) FromAST(node *Node) {
	switch node.Type {
	case ruleFilter:
		filter := &Filter{}
		filter.FromAST(node)
		p.Filters = append(p.Filters, filter)
	default:
		for _, child := range node.Children {
			p.FromAST(child)
		}
	}
}

type Order struct {
	Column        string
	Direction     string
	NullDirection string
}

type Filter struct {
	Field    string
	Not      string
	Operator string
	Operands []string
	AnyAll   string
}

func (filter *Filter) FromAST(n *Node) {

	for _, child := range n.Children {

		if child.Type == ruleColumnName {
			filter.Field = child.Value
		}

		if child.Type == rulePredicate {
			for _, pred := range child.Children {
				if pred.Type == ruleNot {
					filter.Not = "NOT"
				}
				if pred.Type == ruleOperator {
					filter.Operator = pred.Value
				}

				if pred.Type == ruleOperand {
					if pred.Children[0].Type == ruleVectorOperand {
						for _, operand := range pred.Children[0].Children {
							unquoted, err := strconv.Unquote(operand.Value)
							if err == nil {
								filter.Operands = append(filter.Operands, unquoted)
							} else {
								filter.Operands = append(filter.Operands, operand.Value)
							}
						}

					} else if pred.Children[0].Type == ruleScalarOperand {
						unquoted, err := strconv.Unquote(pred.Children[0].Value)
						if err == nil {
							filter.Operands = append(filter.Operands, unquoted)
						} else {
							filter.Operands = append(filter.Operands, pred.Children[0].Value)
						}
					}

				}
			}
		}
	}
}
