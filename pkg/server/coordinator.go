package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
)

// Task - задача от координатора воркеру
type Task struct {
	Lines        []string `json:"lines"`          // строки для обработки
	StartLineNum int      `json:"start_line_num"` // номер первой строки в слайсе (для глобальной нумерации)
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
	Lines []string `json:"lines,omitempty"` // строки (если не флаг -c)
	Count int      `json:"count,omitempty"` // количество совпадений (если флаг -c)
	Error string   `json:"error,omitempty"` // текст ошибки
}

// Coordinator реализует распределённую обработку с репликацией и кворумом
// (каждому воркеру отправляется одна и та же задача (все строки), координатор ждёт, пока большинство
// воркеров (N/2+1) вернут успешный результат, после чего выводит результат и завершает работу)
func Coordinator(ctx context.Context, cfg *configuration.Config, inputReader io.Reader) error {

	slog.Info("Запуск координатора в распределённом режиме", "cluster", cfg.ClusterAddrs)

	// 1. Читаем все строки из входного потока (файл или stdin)
	scanner := bufio.NewScanner(inputReader)
	lines := make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ошибка чтения входных данных: %w", err)
	}

	slog.Info("Прочитано строк", "count", len(lines))

	// 2. Формируем задачу для воркеров (одинаковую для всех)
	task := Task{
		Lines:        lines,
		StartLineNum: 1, // нумерация начинается с 1
		After:        cfg.After,
		Before:       cfg.Before,
		Context:      cfg.Context,
		Count:        cfg.Count,
		IgnoreCase:   cfg.IgnoreCase,
		Invert:       cfg.Invert,
		Fixed:        cfg.Fixed,
		LineNumber:   cfg.LineNumber,
		Pattern:      cfg.Pattern,
	}

	// 3. Создаём отменяемый контекст для горутин
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	results := make(chan *Result, len(cfg.ClusterAddrs))

	var wg sync.WaitGroup

	for _, addr := range cfg.ClusterAddrs {

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			res, err := sendTaskToWorker(workerCtx, addr, task)
			if err != nil {
				slog.Error("Ошибка при обращении к воркеру", "addr", addr, "error", err)
				res = &Result{Error: err.Error()}
			}

			// пытаемся отправить результат, но если контекст отменён (кворум достигнут), то не блокируемся
			select {
			case results <- res:
			case <-workerCtx.Done():
				// координатор уже завершил ожидание, просто выходим
			}
		}(addr)
	}

	// 4. Кворум и таймаут
	quorum := len(cfg.ClusterAddrs)/2 + 1
	successCount := 0
	firstSuccess := &Result{}

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second)
	defer timeoutCancel()

	for successCount < quorum {

		select {
		case res := <-results:
			if res.Error == "" {
				successCount++
				if firstSuccess == nil {
					firstSuccess = res
				}
				slog.Info("Получен успешный ответ", "progress", fmt.Sprintf("%d/%d", successCount, quorum))
			} else {
				slog.Warn("Получен ошибочный ответ", "error", res.Error)
			}

		case <-timeoutCtx.Done():
			workerCancel() // отменяем воркеров, чтобы они не висели
			wg.Wait()
			return fmt.Errorf("не достигнут кворум за отведённое время: получено %d из %d успешных ответов", successCount, quorum)

		case <-ctx.Done():
			workerCancel()
			wg.Wait()
			return fmt.Errorf("координатор прерван")
		}
	}

	// 5. Кворум достигнут - отменяем остальные горутины
	workerCancel()
	wg.Wait() // дожидаемся, чтобы все горутины завершились
	close(results)

	// 6. Выводим результат
	if firstSuccess == nil {
		return fmt.Errorf("нет успешных результатов")
	}
	if cfg.Count {
		// для флага -c все успешные ответы должны содержать одинаковое количество
		fmt.Printf("%d\n", firstSuccess.Count)
	} else {
		for _, line := range firstSuccess.Lines {
			fmt.Println(line)
		}
	}

	return nil
}

// sendTaskToWorker отправляет задачу по указанному адресу и возвращает результат
func sendTaskToWorker(ctx context.Context, addr string, task Task) (*Result, error) {

	// устанавливаем соединение с таймаутом из контекста
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("не удалось подключиться к %s: %w", addr, err)
	}
	defer conn.Close()

	// сериализуем задачу в JSON
	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("ошибка сериализации задачи: %w", err)
	}

	// отправляем длину сообщения (4 байта)
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))
	if _, err := conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("ошибка отправки длины: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("ошибка отправки задачи: %w", err)
	}

	// читаем ответ: сначала 4 байта длины
	lenBufResp := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBufResp); err != nil {
		return nil, fmt.Errorf("ошибка чтения длины ответа: %w", err)
	}

	respLen := int(uint32(lenBufResp[0])<<24 | uint32(lenBufResp[1])<<16 | uint32(lenBufResp[2])<<8 | uint32(lenBufResp[3]))
	if respLen <= 0 || respLen > 10*1024*1024 { // защита от слишком больших сообщений
		return nil, fmt.Errorf("неверная длина ответа: %d", respLen)
	}

	respData := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respData); err != nil {
		return nil, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	// парсим JSON в Result
	result := &Result{}
	if err := json.Unmarshal(respData, &result); err != nil {
		return nil, fmt.Errorf("ошибка разбора JSON ответа: %w", err)
	}
	if result.Error != "" {
		return result, fmt.Errorf("воркер вернул ошибку: %s", result.Error)
	}

	return result, nil
}
