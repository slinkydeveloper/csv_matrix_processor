package pkg

import (
	"fmt"
	"github.com/montanaflynn/stats"
	"strconv"
	"time"
)

type percentileOperation struct {
	keyColumn int
	valueColumn int
	percentile float64
	outputColumn int
}

func (p percentileOperation) String() string {
	return fmt.Sprintf("Percentile %g, key column %d, values column %d, output column %d", p.percentile, p.keyColumn, p.valueColumn, p.outputColumn)
}

func (p percentileOperation) Run(input [][]float64) [][]float64 {
	count := 0

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

	jobs := make(chan int, 32)
	rows := make(chan []float64, 32)

	for j := 0; j < 16; j++ {
		go func() {
			for i := range jobs {
				var err error
				percentile, err := stats.Percentile(values[:i + 1], p.percentile)
				if err != nil {
					panic(err)
				}

				// Create new row for the matrix
				row := make([]float64, p.outputColumn + 1)
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
		r := <- rows
		input = append(input, r)
	}

	close(jobs)
	close(rows)

	return input
}

func worker(id int, jobs <-chan int, results chan<- int) {
	for j := range jobs {
		fmt.Println("worker", id, "started  job", j)
		time.Sleep(time.Second)
		fmt.Println("worker", id, "finished job", j)
		results <- j * 2
	}
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

