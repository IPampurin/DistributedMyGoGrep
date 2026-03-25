package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/local"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
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

	// парсим команду запуска
	cfg, err := configuration.ParseConfig()
	if err != nil {
		slog.Error("Ошибка разбора аргументов", "error", err)
		os.Exit(1)
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
	switch cfg.Mode {

	case configuration.ModeLocal:
		// локальный режим
		slog.Info("Локальный режим")

		result, err := local.GrepLocal(cfg, inputReader)
		if err != nil {
			slog.Error("Локальный grep завершился с ошибкой", "error", err)
			os.Exit(1)
		}

		printResult(cfg, result)

	case configuration.ModeNodes:
		// режим ноды (ждёт подключений)
		slog.Info("Режим сервера (запуск одного или нескольких узлов кластера)", "addrs", cfg.SrvAddrs)
		/*
			// запускаем отдельный сервер для каждого адреса в своей горутине
			var wg sync.WaitGroup
			for i, addr := range cfg.SrvAddrs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := servers.ServerRun(ctx, addr, logger); err != nil {
						mes := fmt.Sprintf("Воркер-сервер %d по адресу %s завершился с ошибкой", i, addr)
						slog.Error(mes, "error", err)
					}
				}()
			}
			wg.Wait()
		*/
		slog.Info("Все воркер-серверы остановлены.")

	case configuration.ModeMaster:
		// режим мастера
		slog.Info("Режим мастера", "cluster", cfg.SrvAddrs)
		/*
			coord := distributed.New(cfg, cfg.SrvAddrs)
			if err := coord.Run(ctx, inputReader); err != nil {
				slog.Error("Координатор завершился с ошибкой", "error", err)
				os.Exit(1)
			}
		*/
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
