package pkg

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func BenchmarkPercentile(b *testing.B) {
	tests := []struct {
		min      float64
		max      float64
		elements uint
	}{
		{
			min:      0,
			max:      1,
			elements: 1000,
		},
		{
			min:      0,
			max:      1000,
			elements: 1000,
		},
		{
			min:      0,
			max:      1,
			elements: 10000,
		},
		{
			min:      0,
			max:      1000,
			elements: 10000,
		},
		{
			min:      0,
			max:      1,
			elements: 50000,
		},
		{
			min:      0,
			max:      1000,
			elements: 50000,
		},
	}
	for _, test := range tests {
		b.Run(fmt.Sprintf("%d elements [%v,%v]", test.elements, test.min, test.max), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkPercentile(test.min, test.max, test.elements)
			}
		})
	}
}

func benchmarkPercentile(min float64, max float64, elements uint) {
	matrix := make([][]float64, elements)
	for i := uint(0); i < elements; i++ {
		matrix[i] = []float64{float64(i), randBoundedFloat64(min, max)}
	}

	percentile := percentileOperation{
		keyColumn:    0,
		valueColumn:  1,
		percentile:   99.9,
		outputColumn: 2,
	}

	_ = percentile.Run(matrix)
}

func randBoundedFloat64(min float64, max float64) float64 {
	return (rand.Float64() * (max - min)) + min
}
