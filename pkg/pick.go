package pkg

import (
	"fmt"
	"strconv"
)

type pickOperation struct {
	columns []int
}

func (p pickOperation) Run(input [][]float64) [][]float64 {
	matchCount := 0
	for _, row := range input {
		if containsColumns(row, p.columns) {
			matchCount++
		}
	}
	matrix := make([][]float64, matchCount)
	i := 0
	for _, row := range input {
		if containsColumns(row, p.columns) {
			matrix[i] = p.compactColumn(row)
			i++
		}
	}
	fmt.Printf("Now matrix is %d x %d -- ", len(matrix), len(p.columns))
	return matrix
}

func (p pickOperation) compactColumn(input []float64) []float64 {
	out := make([]float64, len(p.columns))
	for i, c := range p.columns {
		out[i] = input[c]
	}
	return out
}

func (p pickOperation) String() string {
	return fmt.Sprintf("Pick columns %+v", p.columns)
}

func NewPick(args []string) (Operation, error) {
	columns := make([]int, len(args))

	for i, a := range args {
		c, err := strconv.Atoi(a)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}

	return pickOperation{columns}, nil

}



