package network

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// HTTPServer реализует интерфейс Server для запуска HTTP-сервера, обрабатывающего задачи
type HTTPServer struct {
	server *http.Server
}

// Start запускает HTTP-сервер на указанном адресе и обрабатывает входящие задачи, передавая их в handler
func (s *HTTPServer) Start(ctx context.Context, addr string, handler TaskHandler) error {

	mux := http.NewServeMux()
	mux.HandleFunc("/task", func(w http.ResponseWriter, r *http.Request) {
		// обработка только POST-запросов
		if r.Method != http.MethodPost {
			http.Error(w, "только POST разрешён", http.StatusMethodNotAllowed)
			return
		}

		// читаем тело запроса
		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("ошибка чтения тела запроса", "error", err)
			http.Error(w, "ошибка чтения запроса", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// декодируем задачу
		var task models.Task
		if err := json.Unmarshal(body, &task); err != nil {
			slog.Error("ошибка декодирования задачи", "error", err)
			http.Error(w, "неверный формат задачи", http.StatusBadRequest)
			return
		}

		// вызываем обработчик
		result, err := handler(r.Context(), task)
		if err != nil {
			slog.Error("ошибка обработки задачи", "error", err)
			// возвращаем результат с ошибкой
			result = &models.Result{Error: err.Error()}
		}

		// сериализуем ответ
		respBody, err := json.Marshal(result)
		if err != nil {
			slog.Error("ошибка кодирования ответа", "error", err)
			http.Error(w, "внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}

		// отправляем ответ
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(respBody); err != nil {
			slog.Error("ошибка записи ответа", "error", err)
		}
	})

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// запускаем сервер в горутине
	errCh := make(chan error, 1)
	go func() {
		slog.Info("запуск HTTP-сервера", "addr", addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("ошибка работы сервера: %w", err)
		}
		close(errCh)
	}()

	// ожидаем либо сигнала от контекста, либо ошибки запуска
	select {
	case <-ctx.Done():
		// даём время на завершение текущих запросов (например, 5 секунд)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("ошибка shutdown при остановке сервера: %w", err)
		}
		slog.Info("HTTP-сервер корректно остановлен", "addr", addr)
		return nil

	case err := <-errCh:
		return err
	}
}
