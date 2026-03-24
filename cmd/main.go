package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/distributed"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/local"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/node"
)

func main() {

	// cоздаём контекст
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// запускаем горутину обработки сигналов
	go signalHandler(ctx, cancel)

	// настраиваем логгер
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// парсим флаги
	cfg, _, err := configuration.ParseFlags()
	if err != nil {
		slog.Error("Ошибка разбора аргументов", "error", err)
		os.Exit(1)
	}

	// таймаут для одиночной ноды (из переменной окружения или по умолчанию 5 минут)
	nodeTimeout := 5 * time.Minute
	if timeoutStr := os.Getenv("TIME_OUT_NODE_MYGOGREP"); timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			nodeTimeout = d
		} else {
			slog.Warn("Некорректное значение TIME_OUT_NODE_MYGOGREP, используется значение по умолчанию", "value", timeoutStr, "error", err)
		}
	}

	// определяем источник ввода (файл или stdin)
	var inputReader io.Reader

	if cfg.Filename != "" {
		file, err := os.Open(cfg.Filename)
		if err != nil {
			slog.Error("Не удалось открыть файл", "filename", cfg.Filename, "error", err)
			os.Exit(1)
		}
		defer file.Close()
		inputReader = file
	} else {
		inputReader = os.Stdin
	}

	// выбираем режим работы по количеству адресов в кластере
	switch len(cfg.SrvAddrs) {

	case 0:
		// локальный режим
		slog.Info("Локальный режим")

		if cfg.Pattern == "" {
			slog.Error("Локальный режим: требуется шаблон")
			os.Exit(1)
		}

		result, err := local.Grep(cfg, inputReader)
		if err != nil {
			slog.Error("Локальный grep завершился с ошибкой", "error", err)
			os.Exit(1)
		}

		printResult(cfg, result)

	case 1:
		// режим одиночной ноды (ждёт подключений)
		slog.Info("Режим одиночной ноды", "addr", cfg.SrvAddrs[0])

		workerCtx, workerCancel := context.WithTimeout(ctx, nodeTimeout)
		defer workerCancel()

		if err := node.Run(workerCtx, cfg); err != nil {
			slog.Error("Воркер-сервер завершился с ошибкой", "error", err)
			os.Exit(1)
		}

	default:
		// режим координатора
		slog.Info("Режим координатора", "cluster", cfg.SrvAddrs)

		if cfg.Pattern == "" {
			slog.Error("Распределённый режим: требуется шаблон")
			os.Exit(1)
		}

		coord := distributed.New(cfg, cfg.SrvAddrs)
		if err := coord.Run(ctx, inputReader); err != nil {
			slog.Error("Координатор завершился с ошибкой", "error", err)
			os.Exit(1)
		}
	}

	slog.Info("Программа корректно завершена.")
}

// printResult выводит результат локальной обработки в stdout
func printResult(cfg *configuration.Config, result *models.GrepResult) {

	if cfg.Count {
		fmt.Printf("%d\n", result.Count)
		return
	}

	for _, line := range result.Lines {
		fmt.Printf("%s\n", line)
	}
}

// signalHandler обрабатывет сигналы отмены
func signalHandler(ctx context.Context, cancel context.CancelFunc) {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case <-ctx.Done():
		return
	case <-sigChan:
		cancel()
		return
	}
}
