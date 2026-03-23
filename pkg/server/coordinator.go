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

// Shard - информация о шарде
type Shard struct {
	ID           int        // порядковый номер шарда
	Lines        []string   // строки этого шарда
	StartLineNum int        // глобальный номер первой строки
	SuccessCount int        // сколько успешных ответов получено
	Result       *Result    // первый успешный результат (для вывода)
	Replicas     []string   // адреса воркеров, которым отправлен шард
	mu           sync.Mutex // защита SuccessCount и Result
}

// Coordinator реализует распределённую обработку с шардированием и репликацией, кворумом на каждый шард
// (разбивает входные данные на шарды, каждый шард отправляет на R реплик, ждёт кворума (R/2+1 успехов)
// для каждого шарда, затем объединяет результаты)
func Coordinator(ctx context.Context, cfg *configuration.Config, inputReader io.Reader) error {

	workers := cfg.ClusterAddrs
	numWorkers := len(workers)
	if numWorkers == 0 {
		return fmt.Errorf("нет воркеров для распределённой обработки")
	}

	slog.Info("Запуск координатора с шардированием", "воркеры", workers)

	// 1. Читаем все строки
	scanner := bufio.NewScanner(inputReader)

	allLines := make([]string, 0)
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ошибка чтения входных данных: %w", err)
	}

	totalLines := len(allLines)

	slog.Info("Прочитано строк", "count", totalLines)

	// 2. Определяем параметры шардирования и репликации
	numShards := numWorkers * 2 // количество шардов (можно вынести в конфиг)
	if numShards == 0 {
		numShards = 1
	}

	replicationFactor := 2 // фиксированный фактор репликации, должен быть <= numWorkers
	if replicationFactor > numWorkers {
		replicationFactor = numWorkers
	}
	quorumPerShard := replicationFactor/2 + 1

	// вычисляем размер шарда
	shardSize := (totalLines + numShards - 1) / numShards
	if shardSize < 1 {
		shardSize = 1
	}

	// создаём шарды
	shards := make([]*Shard, 0, numShards)
	for i := 0; i < numShards; i++ {

		start := i * shardSize
		end := start + shardSize
		if end > totalLines {
			end = totalLines
		}
		if start >= totalLines {
			break
		}

		shard := &Shard{
			ID:           i,
			Lines:        allLines[start:end],
			StartLineNum: start + 1, // нумерация строк начинается с 1
			Replicas:     make([]string, 0, replicationFactor),
		}

		shards = append(shards, shard)
	}

	numShards = len(shards)

	slog.Info("Шардирование", "шардов", numShards, "размер_шарда", shardSize, "репликация", replicationFactor, "кворум_на_шард", quorumPerShard)

	// 3. Распределяем реплики: для каждого шарда выбираем replicationFactor разных воркеров по кругу
	//    (для шарда i реплики будут workers[(i+j) % numWorkers] для j=0..replicationFactor-1)
	for i, shard := range shards {
		for j := 0; j < replicationFactor; j++ {

			workerIdx := (i + j) % numWorkers
			shard.Replicas = append(shard.Replicas, workers[workerIdx])
		}

		slog.Debug("Шард", "id", shard.ID, "реплики", shard.Replicas, "строк", len(shard.Lines))
	}

	// 4. Отправляем задачи на все реплики каждого шарда

	// канал для уведомлений о завершении обработки шарда (содержит ID шарда и результат)
	shardDone := make(chan struct {
		shardID int
		result  *Result
	}, numShards*replicationFactor) // буфер на все возможные ответы

	// создаём отменяемый контекст для прерывания при таймауте
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var wg sync.WaitGroup

	for _, shard := range shards {
		for _, addr := range shard.Replicas {
			wg.Add(1)
			go func(shard *Shard, addr string) {
				defer wg.Done()

				// формируем задачу для этого шарда
				task := Task{
					Lines:        shard.Lines,
					StartLineNum: shard.StartLineNum,
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

				res, err := sendTaskToWorker(workerCtx, addr, task)
				if err != nil {
					slog.Error("Ошибка при обращении к воркеру", "шард", shard.ID, "addr", addr, "error", err)
					res = &Result{Error: err.Error()}
				}

				// отправляем результат в канал
				select {
				case shardDone <- struct {
					shardID int
					result  *Result
				}{shard.ID, res}:
				case <-workerCtx.Done():
					// координатор уже завершил работу
				}
			}(shard, addr)
		}
	}

	// 5. Собираем результаты, обновляем состояние шардов, ждём кворума для каждого
	shardResults := make([]*Result, numShards) // итоговый результат для каждого шарда
	shardCompleted := make([]bool, numShards)  // флаг, что для шарда получен кворум
	remainingShards := numShards

	// таймаут на всю операцию (например, 30 секунд)
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second)
	defer timeoutCancel()

	for remainingShards > 0 {
		select {
		case notif := <-shardDone:
			shard := shards[notif.shardID]
			shard.mu.Lock()
			// если для шарда уже достигнут кворум, игнорируем дополнительные ответы
			if shardCompleted[shard.ID] {
				shard.mu.Unlock()
				continue
			}
			if notif.result.Error == "" {
				shard.SuccessCount++
				if shard.Result == nil {
					shard.Result = notif.result // запоминаем первый успешный результат
				}
			} else {
				slog.Warn("Ошибочный ответ для шарда", "шард", shard.ID, "error", notif.result.Error)
			}
			// проверяем, достигнут ли кворум
			if shard.SuccessCount >= quorumPerShard {
				shardCompleted[shard.ID] = true
				remainingShards--
				shardResults[shard.ID] = shard.Result
				slog.Info("Шард обработан", "шард", shard.ID, "успешно", shard.SuccessCount)
			}
			shard.mu.Unlock()

		case <-timeoutCtx.Done():
			workerCancel() // останавливаем все горутины
			wg.Wait()
			return fmt.Errorf("таймаут: не удалось обработать все шарды за отведённое время")
		case <-ctx.Done():
			workerCancel()
			wg.Wait()
			return fmt.Errorf("координатор прерван")
		}
	}

	// 6. После обработки всех шардов отменяем контекст, чтобы горутины не висели
	workerCancel()
	wg.Wait()
	close(shardDone)

	// 7. Агрегируем результаты всех шардов в правильном порядке
	if cfg.Count {
		totalCount := 0
		for i, res := range shardResults {
			if res == nil {
				return fmt.Errorf("шард %d не имеет результата", i)
			}
			totalCount += res.Count
		}
		fmt.Printf("%d\n", totalCount)
	} else {
		// Выводим строки из каждого шарда в порядке шардов
		for i, res := range shardResults {
			if res == nil {
				return fmt.Errorf("шард %d не имеет результата", i)
			}
			for _, line := range res.Lines {
				fmt.Println(line)
			}
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
