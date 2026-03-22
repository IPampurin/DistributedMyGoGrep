package service

import (
	"bufio"
	"fmt"
	"io"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
)

// GrepResult содержит результат обработки (строки или количество)
type GrepResult struct {
	Lines   []string // строки для вывода (с учётом флагов -n и т.д.)
	Count   int      // количество совпадений (если флаг -c)
	IsCount bool     // true если результат - это счётчик
}

// LocalGrep выполняет grep локально (один узел)
func LocalGrep(cfg *configuration.Config, input io.Reader) (*GrepResult, error) {

	// считываем все строки в слайс
	scanner := bufio.NewScanner(input)

	contents := make([]string, 0)

	for scanner.Scan() {
		contents = append(contents, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("ошибка чтения входных данных: %w", err)
	}

	// вызываем ProcessLines с начальным номером строки 1
	return ProcessLines(cfg, contents, 1)
}
