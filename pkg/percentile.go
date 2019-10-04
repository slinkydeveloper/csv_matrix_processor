package pkg

import (
	"fmt"
	"github.com/influxdata/tdigest"
	"runtime"
	"sort"
	"strconv"
)

type percentileOperation struct {
	keyColumn    int
	valueColumn  int
	percentile   float64
	outputColumn int
}

func (p percentileOperation) String() string {
	return fmt.Sprintf("Percentile %g, key column %d, values column %d, output column %d", p.percentile, p.keyColumn, p.valueColumn, p.outputColumn)
}

func (p percentileOperation) Run(input [][]float64) [][]float64 {
	count := 0

	quantile := p.percentile / 100

	for _, row := range input {
		if containsColumns(row, []int{p.keyColumn, p.valueColumn}) {
			count++
		}
	}

	keys := make([]float64, count)
	values := make([]float64, count)

	i := 0
	for _, row := range input {
		if containsColumns(row, []int{p.keyColumn, p.valueColumn}) {
			keys[i] = row[p.keyColumn]
			values[i] = row[p.valueColumn]
			i++
		}
	}

	// Reorder the values to match the keys order
	sort.Slice(values, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	// Now reorder the keys
	sort.Float64s(keys)

	goroutines := runtime.NumCPU()

	jobs := make(chan int, goroutines*2)
	rows := make(chan []float64, goroutines*2)

	for j := 0; j < goroutines; j++ {
		go func() {
			td := tdigest.New()
			td.Add(values[0], 1)
			lastAddedValue := 0

			for i := range jobs {
				for k := lastAddedValue + 1; k <= i; k++ {
					td.Add(values[k], 1)
				}
				lastAddedValue = i

				percentile := td.Quantile(quantile) * 100

				// Create new row for the matrix
				row := make([]float64, p.outputColumn+1)
				row[p.keyColumn] = keys[i]
				row[p.outputColumn] = percentile
				rows <- row
			}
		}()
	}

	go func() {
		for i := 1; i < len(keys); i++ {
			jobs <- i
		}
	}()

	for i := 1; i < len(keys); i++ {
		r := <-rows
		input = append(input, r)
	}

	close(jobs)
	close(rows)

	return input
}

func NewPercentile(args []string) (Operation, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("Invalid number of args for percentile. Required: 4, actual %d", len(args))
	}

	keyColumn, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, err
	}

	valueColumn, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, err
	}

	percentile, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return nil, err
	}

	outputColumn, err := strconv.Atoi(args[3])
	if err != nil {
		return nil, err
	}

	return percentileOperation{keyColumn, valueColumn, percentile, outputColumn}, nil
}
