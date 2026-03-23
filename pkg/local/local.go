package local

import (
	"bufio"
	"fmt"
	"io"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/service"
)

// Grep выполняет grep локально (один узел)
func Grep(cfg *configuration.Config, input io.Reader) (*models.GrepResult, error) {

	// считываем все строки в слайс
	scanner := bufio.NewScanner(input)

	lines := make([]string, 0)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("ошибка чтения входных данных: %w", err)
	}

	// вызываем ProcessLines с начальным номером строки 1
	return service.ProcessLines(cfg, lines, 1)
}
