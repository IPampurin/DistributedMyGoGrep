package worker

import (
	"context"
	"fmt"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/service"
)

// Handler возвращает функцию-обработчик задач
// (преобразует полученную задачу в формат, понятный service.ProcessLines, и возвращает результат)
func Handler() func(ctx context.Context, task models.Task) (*models.Result, error) {
	return func(ctx context.Context, task models.Task) (*models.Result, error) {

		// создаём временный конфиг из полей задачи
		taskCfg := &configuration.Config{
			After:      task.After,
			Before:     task.Before,
			Context:    task.Context,
			Count:      task.Count,
			IgnoreCase: task.IgnoreCase,
			Invert:     task.Invert,
			Fixed:      task.Fixed,
			LineNumber: task.LineNumber,
			Pattern:    task.Pattern,
		}

		// вызываем ProcessLines с переданными строками и номером первой строки
		grepResult, err := service.ProcessLines(taskCfg, task.Lines, task.StartLineNum)
		if err != nil {
			return nil, fmt.Errorf("ошибка обработки строк: %w", err)
		}

		// формируем Result из GrepResult
		result := &models.Result{}
		if grepResult.IsCount {
			result.Count = grepResult.Count
		} else {
			result.Lines = grepResult.Lines
		}

		return result, nil
	}
}
