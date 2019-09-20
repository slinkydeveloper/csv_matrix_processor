package pkg

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

type orderOperation struct {
	referenceColumn int
	compact bool
}

func (c orderOperation) String() string {
	if c.compact {
		return fmt.Sprintf("Order and compact by column %d", c.referenceColumn)
	} else {
		return fmt.Sprintf("Order by column %d", c.referenceColumn)
	}
}

func (c orderOperation) Run(input [][]float64) [][]float64 {
	sort.Slice(input, func(i, j int) bool {
		return input[i][c.referenceColumn] < input[j][c.referenceColumn]
	})
	if c.compact {
		output := make([][]float64, 0)
		for i := 0; i < len(input); {
			var actual = input[i][c.referenceColumn]
			var newRow = input[i]
			j := i + 1
			for {
				if j >= len(input) {
					break
				}
				next := input[j][c.referenceColumn]
				if actual != next {
					break
				}
				newRow = merge(newRow, input[j])
				j++
			}
			i = j
			output = append(output, newRow)
		}
		return output
	} else {
		return input
	}
}

func merge(v1, v2 []float64) []float64 {
	maxLength := int(math.Max(float64(len(v1)), float64(len(v2))))
	out := make([]float64, maxLength)

	for i := 0; i < maxLength; i++ {
		if i >= len(v1) {
			out[i] = v2[i]
		} else if i >= len(v2) {
			out[i] = v1[i]
		} else if v1[i] != math.NaN() {
			out[i] = v1[i]
		} else {
			out[i] = v2[i]
		}
	}

	return out
}

func NewOrder(args []string) (Operation, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("Invalid number of args for order. Required: 1, found: %v", len(args))
	}

	ref, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, err
	}

	return orderOperation{ref, false}, nil
}

func NewOrderAndCompact(args []string) (Operation, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("Invalid number of args for order. Required: 1, found: %v", len(args))
	}

	ref, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, err
	}

	return orderOperation{ref, true}, nil
}