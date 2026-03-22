package service

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
)

// ProcessLines обрабатывает готовый слайс строк (без чтения из ридера)
// startLineNum - номер первой строки в слайсе (для глобальной нумерации)
func ProcessLines(cfg *configuration.Config, lines []string, startLineNum int) (*GrepResult, error) {

	// подготавливаем регулярное выражение
	pattern := cfg.Pattern
	if cfg.IgnoreCase {
		pattern = "(?i)" + pattern
	}
	if cfg.Fixed {
		pattern = regexp.QuoteMeta(pattern)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("неверный шаблон: %w", err)
	}

	// определяем контекст
	before := cfg.Before
	after := cfg.After
	if cfg.Context > 0 {
		before = cfg.Context
		after = cfg.Context
	}

	// проверяем совпадения
	matches := make([]bool, len(lines))
	for i, line := range lines {
		matched := re.MatchString(line)
		if cfg.Invert {
			matched = !matched
		}
		matches[i] = matched
	}

	// если нужен только счётчик
	if cfg.Count {
		count := 0
		for _, matched := range matches {
			if matched {
				count++
			}
		}

		return &GrepResult{
				Count:   count,
				IsCount: true},
			nil
	}

	// собираем индексы строк для вывода (с контекстом)
	outputIndexes := make(map[int]bool)
	for i, matched := range matches {
		if matched {
			start := i - before
			if start < 0 {
				start = 0
			}
			for j := start; j <= i; j++ {
				outputIndexes[j] = true
			}
			end := i + after
			if end >= len(lines) {
				end = len(lines) - 1
			}
			for j := i + 1; j <= end; j++ {
				outputIndexes[j] = true
			}
		}
	}

	// сортируем индексы
	sortedIndexes := make([]int, 0)
	for idx := range outputIndexes {
		sortedIndexes = append(sortedIndexes, idx)
	}
	sort.Ints(sortedIndexes)

	// формируем результат (строки с номерами, если нужно)
	resultLines := make([]string, 0, len(sortedIndexes))
	for _, idx := range sortedIndexes {
		line := lines[idx]
		if cfg.LineNumber {
			resultLines = append(resultLines, fmt.Sprintf("%d:%s", startLineNum+idx, line))
		} else {
			resultLines = append(resultLines, line)
		}
	}

	return &GrepResult{
			Lines:   resultLines,
			IsCount: false},
		nil
}
