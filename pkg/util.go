package pkg

import "math"

func containsColumns(input []float64, columns []int) bool {
	for _, c := range columns {
		if c >= len(input) || input[c] == math.NaN() || input[c] == 0 {
			return false
		}
	}
	return true
}
