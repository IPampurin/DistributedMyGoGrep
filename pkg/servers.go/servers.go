package servers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// ServerRun запускает сервер по указанному адресу до отмены контекста
func ServerRun(ctx context.Context, addr string, logger *slog.Logger) error {

	if addr == "" {
		return fmt.Errorf("отсутствует адрес сервера")
	}

	// определяем сервер
	srv := &http.Server{
		Addr: addr,
	}

	// канал для ошибок от сервера
	errCh := make(chan error, 1)

	// запускаем сервер в горутине
	go func() {
		logger.Info("запуск HTTP-сервера", "address", addr)
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// ожидаем либо сигнала от контекста, либо ошибки запуска
	select {
	case <-ctx.Done():
		logger.Info("получен сигнал завершения, останавливаем сервер...")
		// даём время на завершение текущих запросов (например, 5 секунд)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("ошибка при graceful shutdown", "error", err)
			return err
		}
		logger.Info("сервер корректно остановлен")
		return nil

	case err := <-errCh:
		logger.Error("сервер завершился с ошибкой", "error", err)
		return err
	}
}
