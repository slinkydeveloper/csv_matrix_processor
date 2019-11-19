package pkg

import (
	"fmt"
	"strconv"
)

type skipOperation struct {
	column          int
	referenceNumber float64
}

func (c skipOperation) String() string {
	return fmt.Sprintf("Skip rows by column %d and value %f", c.column, c.referenceNumber)
}

func (c skipOperation) Run(input [][]float64) [][]float64 {
	output := make([][]float64, 0)

	for i := 0; i < len(input); i++ {
		if containsColumns(input[i], []int{c.column}) && input[i][c.column] >= c.referenceNumber {
			output = append(output, input[i])
		}
	}
	return output
}

func NewSkip(args []string) (Operation, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("Invalid number of args for skip. Required: 2, found: %v", len(args))
	}

	column, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, err
	}

	referenceNumber, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return nil, err
	}

	return skipOperation{column: column, referenceNumber: referenceNumber}, nil
}
