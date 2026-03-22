package server

import (
	"context"
	"log/slog"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
)

// Coordinator запускает координатора, который управляет распределённой обработкой
// (разбивает входные данные на шарды, распределяет по узлам, ждёт кворума, собирает результат и выводит его)
func Coordinator(ctx context.Context, cfg *configuration.Config, inputReader interface{}) error {

	slog.Info("Starting coordinator in distributed mode", "cluster", cfg)

	// TODO: реализовать логику координатора

	return nil
}

// WorkerServer запускает узел-воркер, который обрабатывает полученные задачи
// (вызывается координатором для каждого узла в отдельном процессе)
func WorkerServer(ctx context.Context, cfg *configuration.Config) error {

	slog.Info("Starting worker server", "port", cfg)

	// TODO: реализовать сетевой сервер, который принимает задачи и возвращает результаты

	return nil
}
