package pkg

import (
	"fmt"
	"strings"
)

type Operation interface {
	Run(input [][]float64)[][]float64
	fmt.Stringer
}

type operationParser struct {
	operationFactories map[string]func([]string)(Operation, error)
}

var operationParserInstance operationParser

func init() {
	operationParserInstance = operationParser{
		operationFactories: map[string]func([]string) (Operation, error){
			"order":      NewOrder,
			"scale":      NewScale,
			"percentile": NewPercentile,
			"order_and_compact": NewOrderAndCompact,
			"pick": NewPick,
		},
	}
}

func Parse(command string) (Operation, error) {
	c := strings.Split(command, " ")
	operationFactory, ok := operationParserInstance.operationFactories[c[0]]
	if !ok {
		return nil, fmt.Errorf("Cannot find command %s", c[0])
	}
	return operationFactory(c[1:])
}