package service

import (
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

	// TODO: реализовать. Пока заглушка

	return nil, nil
}
