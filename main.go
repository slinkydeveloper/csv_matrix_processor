package main

import (
	"bufio"
	"fmt"
	"github.com/slinkydeveloper/csv_processor/pkg"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func main() {
	filename := os.Args[1]
	outFilename := os.Args[2]

	if !fsExist(filename) {
		panic(fmt.Sprintf("Cannot find %s", filename))
	}

	operations, err := readOperations(os.Stdin)
	if err != nil {
		panic(err)
	}

	inFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer inFile.Close()

	fmt.Printf("Reading %s\n", filename)

	matrix, err := readCsv(inFile)
	if err != nil {
		panic(err)
	}

	fmt.Printf("CSV rows %d\n", len(matrix))

	startOps := time.Now()

	for _, op := range operations {
		fmt.Printf("%+v -- ", op)
		start := time.Now()
		matrix = op.Run(matrix)
		fmt.Printf("elapsed time: %v\n", time.Now().Sub(start))
		runtime.GC()
	}

	fmt.Printf("Process time: %v\n", time.Now().Sub(startOps))

	outFile, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	err = writeCsv(outFile, matrix)
	if err != nil {
		panic(err)
	}

	outFile.Sync()

	fmt.Printf("Results wrote in %s\n", outFilename)
}

func readOperations(reader io.Reader) ([]pkg.Operation, error) {
	operations := make([]pkg.Operation, 0)

	bufReader := bufio.NewReader(reader)

	for {
		line, err := bufReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return operations, nil
			} else {
				return nil, err
			}
		}
		op, err := pkg.Parse(line[:len(line) - 1])
		if err != nil {
			return nil, err
		}
		operations = append(operations, op)
	}
}

func readCsv(reader io.Reader) ([][]float64, error) {
	matrix := make([][]float64, 0)

	bufReader := bufio.NewReader(reader)

	for {
		line, err := bufReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return matrix, nil
			} else {
				return nil, err
			}
		}
		if strings.HasPrefix(line, "#") {
			continue
		}

		unparsedRow := strings.Split(line[:len(line) - 1], ",")

		row := make([]float64, len(unparsedRow))

		for i, str := range unparsedRow {
			if str != "" {
				v, err := strconv.ParseFloat(str, 64)
				if err != nil {
					row[i] = math.NaN()
				} else {
					row[i] = v
				}
			} else {
				row[i] = 0
			}
		}

		matrix = append(matrix, row)
	}
}

func writeCsv(fileWriter io.Writer, matrix [][]float64) error {
	w := bufio.NewWriter(fileWriter)

	for _, row := range matrix {
		rowStr := make([]string, len(row))
		for i, el := range row {
			if el == math.NaN() {
				rowStr[i] = ""
			} else {
				rowStr[i] = fmt.Sprintf("%f", el)
			}
		}

		rowToWrite := fmt.Sprintf("%s\n", strings.Join(rowStr, ","))

		_, err := w.WriteString(rowToWrite)
		w.Flush()

		if err != nil {
			return err
		}
	}
	return nil
}

func fsExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}