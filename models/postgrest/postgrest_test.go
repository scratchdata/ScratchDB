package postgrest

import (
	"testing"
)

type PostgrestTest struct {
	Expression string
	IsInvalid  bool
	Print      bool
}

var TestCases []PostgrestTest = []PostgrestTest{
	{Expression: "name=eqJohn", IsInvalid: true},
	{Expression: "name=eq.John"},
	{Expression: "name=not.eq.1"},
	{Expression: "name=not.eq.(1)"},
	{Expression: "name=not.eq.(1,2)"},
	{Expression: `name=not.eq.(1,2,"3",3.14,a,b.c,"d.e)","f,","g\"h")`},
	{Expression: `name=eq(any).{1,"2",3}`},
	{Expression: `limit=&name=eq.John`, IsInvalid: true},
	{Expression: `limit=123&name=eq.John`},
	{Expression: `"my column"=eq.John`},
	{Expression: `limit=123&name=eq.John&offset=5`, Print: false},
	{Expression: `"my column"=eq.John`},
	{Expression: `name=eq.John&order=age`},
	{Expression: `name=eq.John&order=age,height`, Print: false},
	{Expression: `name=eq.John&order=age.asc,height.desc`},
	{Expression: `name=eq.John&order=age.nullsfirst,height.desc,weight,country.desc.nullslast`, Print: false},
	{Expression: `name=eq.John&order=age.asc.nullsfirst`},
	{Expression: `select=a`, Print: false},
	{Expression: `select=a,b,c`, Print: false},
	{Expression: `select=a,renamed:b,"Renamed Column":c`, Print: false},
	{Expression: `select=renamed:a`, Print: false},
	{Expression: `select=renamed:count()`, Print: false},
	{Expression: `select=count()`, Print: false},
	{Expression: `select=a::int`, Print: false},
	{Expression: `select=renamed:count()::string`, Print: false},
	{Expression: `select=renamed:a::string,b::string`, Print: false},
	{Expression: `select=a.sum()`, Print: false},
	{Expression: `select=a::int.sum()`, Print: false},
	{Expression: `select=a::int.sum()::float`, Print: false},
	{Expression: `select=a.count(distinct)`, Print: false},
	{Expression: `not.and=(a.eq.1,b.not.neq.(2,3))`, Print: false},
	{Expression: `or=(a.eq.1,and(b.eq.2,c.not.gte.5),not.or(name.ilike(all).{x,y,z}))`, Print: false},
	{Expression: `grade=gte.90&student=is.true&or=(age.eq.14,not.and(age.gte.11,age.lte.17))`, Print: true},
}

func TestPEG(t *testing.T) {

	for _, testCase := range TestCases {
		t.Run(testCase.Expression, func(t *testing.T) {
			parser := &PostgrestParser{Buffer: string(testCase.Expression)}
			err := parser.Init()
			if err != nil {
				t.Error(err)
			}

			err = parser.Parse()
			if testCase.IsInvalid && err == nil {
				t.Error("Should have been an error")
			}

			if !testCase.IsInvalid && err != nil {
				t.Error(err)
			}

			root := &Node{}
			PopulateAST(testCase.Expression, root, parser.AST())

			if testCase.Print {
				parser.PrettyPrintSyntaxTree(testCase.Expression)

				// j, err := json.MarshalIndent(root, "", "    ")
				// if err != nil {
				// 	t.Error(err)
				// }
				// log.Println(string(j))
			}
		})
	}

}
