package pkg

import (
	"fmt"
	"strconv"
)

type scaleOperation struct {
	column int
	factor float64
}

func (s scaleOperation) String() string {
	return fmt.Sprintf("Scale column %d x %g", s.column, s.factor)
}

func (s scaleOperation) Run(input [][]float64) [][]float64 {
	for _, row := range input {
		if len(row) >= s.column + 1 {
			row[s.column] = row[s.column] * s.factor
		}
	}
	return input
}

func NewScale(args []string) (Operation, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("Invalid number of args for scale. Required: 2")
	}

	column, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, err
	}

	scale, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return nil, err
	}

	return scaleOperation{column, scale}, nil
}