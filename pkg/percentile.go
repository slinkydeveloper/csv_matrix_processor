package pkg

import (
	"fmt"
	"strconv"

	"github.com/influxdata/tdigest"
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

	td := tdigest.New()
	td.Add(values[0], 1)

	for i := 1; i < len(keys); i++ {
		td.Add(values[i], 1)

		row := make([]float64, p.outputColumn+1)
		row[p.keyColumn] = keys[i]
		row[p.outputColumn] = td.Quantile(quantile)
		input = append(input, row)
	}

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
