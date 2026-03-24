package node

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/network"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/service"
)

// Run запускает воркер-сервер на адресе из cfg.ClusterAddrs[0]
func Run(ctx context.Context, cfg *configuration.Config) error {

	if len(cfg.SrvAddrs) == 0 {
		return fmt.Errorf("не указан адрес для воркер-сервера")
	}
	addr := cfg.SrvAddrs[0]

	handler := func(task models.Task) (*models.Result, error) {
		// конвертируем Task в конфиг для ProcessLines
		workerCfg := &configuration.Config{
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

		res, err := service.ProcessLines(workerCfg, task.Lines, task.StartLineNum)
		if err != nil {
			return nil, err
		}

		if res.IsCount {
			return &models.Result{Count: res.Count}, nil
		}

		return &models.Result{Lines: res.Lines}, nil
	}

	server := &network.TCPServer{}

	slog.Info("Запуск воркер-сервера", "addr", addr)

	return server.Start(ctx, addr, handler)
}
