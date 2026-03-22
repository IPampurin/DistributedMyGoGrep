package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/service"
)

// Task - задача от координатора воркеру
type Task struct {
	Lines        []string `json:"lines"`
	StartLineNum int      `json:"start_line_num"`
	// флаги grep (из конфига)
	After      int    `json:"after"`
	Before     int    `json:"before"`
	Context    int    `json:"context"`
	Count      bool   `json:"count"`
	IgnoreCase bool   `json:"ignore_case"`
	Invert     bool   `json:"invert"`
	Fixed      bool   `json:"fixed"`
	LineNumber bool   `json:"line_number"`
	Pattern    string `json:"pattern"`
}

// Result - результат от воркера координатору
type Result struct {
	Lines []string `json:"lines,omitempty"`
	Count int      `json:"count,omitempty"`
	Error string   `json:"error,omitempty"`
}

// Coordinator запускает координатора, который управляет распределённой обработкой
// (разбивает входные данные на шарды, распределяет по узлам, ждёт кворума, собирает результат и выводит его)
func Coordinator(ctx context.Context, cfg *configuration.Config, inputReader interface{}) error {

	slog.Info("Starting coordinator in distributed mode", "cluster", cfg)

	// TODO: реализовать логику координатора

	<-ctx.Done()
	return nil
}

// WorkerServer запускает узел-воркер, который обрабатывает полученные задачи
// (вызывается координатором для каждого узла в отдельном процессе)
func WorkerServer(ctx context.Context, cfg *configuration.Config) error {

	if len(cfg.ClusterAddrs) == 0 {
		return fmt.Errorf("не указан адрес для воркер-сервера")
	}

	addr := cfg.ClusterAddrs[0]

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("не удалось запустить слушатель на %s: %w", addr, err)
	}
	defer listener.Close()

	slog.Info("Воркер-сервер запущен", "addr", addr)

	var wg sync.WaitGroup

	// горутина для приёма соединений
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return // нормальное завершение
				default:
					slog.Error("Ошибка при приёме соединения", "error", err)
					continue
				}
			}

			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()

				handleWorkerConn(conn)

			}(conn)
		}
	}()

	// ожидаем отмены контекста (таймаут или сигнал)
	<-ctx.Done()
	slog.Info("Завершение воркер-сервера, ожидание окончания активных соединений")

	// даём время на завершение обработки активных соединений
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("Все соединения обработаны")
	case <-timeoutCtx.Done():
		slog.Warn("Таймаут ожидания завершения соединений")
	}

	return nil
}

// handleWorkerConn обрабатывает одно входящее соединение
func handleWorkerConn(conn net.Conn) {

	defer conn.Close()

	slog.Info("Новое соединение", "remote", conn.RemoteAddr())

	// читаем задачу
	task, err := readTask(conn)
	if err != nil {
		slog.Error("Ошибка чтения задачи", "error", err)
		sendResult(conn, Result{Error: err.Error()})
		return
	}

	// выполняем обработку
	res, err := processTask(task)
	if err != nil {
		sendResult(conn, Result{Error: err.Error()})
		return
	}

	sendResult(conn, *res)
}

// readTask читает из соединения задачу (длина + JSON)
func readTask(conn net.Conn) (*Task, error) {

	// читаем первые 4 байта - длину сообщения (big endian)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("ошибка чтения длины сообщения: %w", err)
	}

	msgLen := int(uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3]))
	if msgLen <= 0 || msgLen > 10*1024*1024 { // ограничим 10 MB
		return nil, fmt.Errorf("неверная длина сообщения: %d", msgLen)
	}

	msg := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msg); err != nil {
		return nil, fmt.Errorf("ошибка чтения тела сообщения: %w", err)
	}

	task := &Task{}
	if err := json.Unmarshal(msg, &task); err != nil {
		return nil, fmt.Errorf("ошибка разбора JSON: %w", err)
	}

	return task, nil
}

// sendResult отправляет результат в соединение (длина + JSON)
func sendResult(conn net.Conn, res Result) error {

	data, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("ошибка сериализации результата: %w", err)
	}

	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("ошибка отправки длины: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("ошибка отправки данных: %w", err)
	}

	return nil
}

// processTask выполняет grep над переданными строками
func processTask(task *Task) (*Result, error) {

	// создаём временный конфиг для ProcessLines
	cfg := &configuration.Config{
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

	result, err := service.ProcessLines(cfg, task.Lines, task.StartLineNum)
	if err != nil {
		return nil, err
	}

	if result.IsCount {
		return &Result{Count: result.Count}, nil
	}

	return &Result{Lines: result.Lines}, nil
}
