package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
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
	ID               int             // порядковый номер шарда
	Lines            []string        // строки этого шарда
	StartLineNum     int             // глобальный номер первой строки
	SuccessCount     int             // количество успешных ответов
	Result           *Result         // первый успешный результат (для вывода)
	OriginalReplicas []string        // первоначальные реплики
	UsedWorkers      map[string]bool // все воркеры, которым отправляли
	ExpectedResp     int             // сколько ответов ожидаем (включая перераспределённые)
	mu               sync.Mutex      // защита SuccessCount и Result
}

// Coordinator реализует распределённую обработку с шардированием, репликацией репликацией и перераспределением,
// (разбивает входные данные на шарды, каждый шард отправляет на R реплик, ждёт кворума (R/2+1 успехов)
// для каждого шарда, при необходимости перераспределяет, затем объединяет результаты)
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
	replicationFactor := 2
	if rfStr := os.Getenv("REPLICATION_FACTOR_MYGOGREP"); rfStr != "" {
		if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
			replicationFactor = rf
			if replicationFactor > numWorkers {
				replicationFactor = numWorkers
				slog.Warn("Фактор репликации больше числа воркеров, установлен в", "replicationFactor", replicationFactor)
			}
		} else {
			slog.Warn("Некорректное значение REPLICATION_FACTOR_MYGOGREP, используется значение по умолчанию", "value", rfStr)
		}
	}
	quorumPerShard := replicationFactor/2 + 1

	numShards := numWorkers * 2
	if nsStr := os.Getenv("COUNT_SCHARD_MYGOGREP"); nsStr != "" {
		if ns, err := strconv.Atoi(nsStr); err == nil && ns > 0 {
			numShards = ns
		} else {
			slog.Warn("Некорректное значение COUNT_SCHARD_MYGOGREP, используется значение по умолчанию", "value", nsStr)
		}
	}

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
			ID:               i,
			Lines:            allLines[start:end],
			StartLineNum:     start + 1, // нумерация строк начинается с 1
			OriginalReplicas: make([]string, 0, replicationFactor),
			UsedWorkers:      make(map[string]bool),
		}

		shards = append(shards, shard)
	}

	numShards = len(shards)

	slog.Info("Шардирование", "шардов", numShards, "размер_шарда", shardSize, "репликация", replicationFactor, "кворум_на_шард", quorumPerShard)

	// 3. Распределяем первоначальные реплики: для каждого шарда выбираем replicationFactor разных воркеров
	for i, shard := range shards {
		for j := 0; j < replicationFactor; j++ {

			workerIdx := (i + j) % numWorkers
			addr := workers[workerIdx]
			shard.OriginalReplicas = append(shard.OriginalReplicas, addr)
			shard.UsedWorkers[addr] = true
		}

		shard.ExpectedResp = len(shard.OriginalReplicas)

		slog.Debug("Шард", "id", shard.ID, "реплики", shard.OriginalReplicas, "строк", len(shard.Lines))
	}

	// канал для уведомлений о завершении обработки шарда
	type shardNotify struct {
		shardID int
		result  *Result
	}
	notifyChan := make(chan shardNotify, numShards*replicationFactor*2) // буфер с запасом

	// создаём отменяемый контекст для прерывания при таймауте
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var wg sync.WaitGroup

	// функция отправки задачи на конкретный воркер для шарда
	sendTask := func(shard *Shard, addr string) {

		defer wg.Done()

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

		select {
		case notifyChan <- shardNotify{shard.ID, res}:
		case <-workerCtx.Done():
		}
	}

	// запускаем отправку на все первоначальные реплики
	for _, shard := range shards {
		for _, addr := range shard.OriginalReplicas {
			wg.Add(1)
			go sendTask(shard, addr)
		}
	}

	// 4. Собираем результаты и перераспределяем
	shardCompleted := make([]bool, numShards) // флаг, что шард завершён (кворум достигнут)
	shardResults := make([]*Result, numShards)
	remainingShards := numShards

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second)
	defer timeoutCancel()

	for remainingShards > 0 {

		select {
		case notif := <-notifyChan:
			shard := shards[notif.shardID]
			shard.mu.Lock()
			if shardCompleted[shard.ID] {
				// шард уже завершён, игнорируем лишние ответы
				shard.mu.Unlock()
				continue
			}
			if notif.result.Error == "" {
				shard.SuccessCount++
				if shard.Result == nil {
					shard.Result = notif.result
				}
			} else {
				slog.Warn("Ошибочный ответ для шарда", "шард", shard.ID, "error", notif.result.Error)
			}
			shard.ExpectedResp--
			// проверяем, достигнут ли кворум
			if shard.SuccessCount >= quorumPerShard {
				shardCompleted[shard.ID] = true
				remainingShards--
				shardResults[shard.ID] = shard.Result
				slog.Info("Шард обработан", "шард", shard.ID, "успешно", shard.SuccessCount)
				shard.mu.Unlock()
				continue
			}
			// если ожидаем ещё ответы, просто выходим
			if shard.ExpectedResp > 0 {
				shard.mu.Unlock()
				continue
			}

			// ответы от всех первоначальных реплик получены, но кворум не достигнут,
			// пытаемся перераспределить на другие воркеры
			availableWorkers := make([]string, 0, numWorkers)
			for _, w := range workers {
				if !shard.UsedWorkers[w] {
					availableWorkers = append(availableWorkers, w)
				}
			}

			if len(availableWorkers) == 0 {
				// нет свободных воркеров - шард не может быть обработан
				shard.mu.Unlock()
				return fmt.Errorf("шард %d: не удалось достичь кворума, нет доступных воркеров", shard.ID)
			}

			// выбираем до replicationFactor новых воркеров (но не более доступных)
			newReplicas := replicationFactor
			if newReplicas > len(availableWorkers) {
				newReplicas = len(availableWorkers)
			}

			newWorkers := availableWorkers[:newReplicas]
			for _, w := range newWorkers {
				shard.UsedWorkers[w] = true
			}

			shard.ExpectedResp += newReplicas
			shard.mu.Unlock()

			// запускаем отправку на новые реплики
			for _, addr := range newWorkers {
				wg.Add(1)
				go sendTask(shard, addr)
			}

			slog.Info("Перераспределение шарда", "шард", shard.ID, "новые_воркеры", newWorkers)

		case <-timeoutCtx.Done():
			workerCancel()
			wg.Wait()
			return fmt.Errorf("таймаут: не удалось обработать все шарды за отведённое время")

		case <-ctx.Done():
			workerCancel()
			wg.Wait()
			return fmt.Errorf("координатор прерван")
		}
	}

	// 5. Все шарды обработаны
	workerCancel()
	wg.Wait()
	close(notifyChan)

	// 6. Агрегация и вывод
	if cfg.Count {

		totalCount := 0
		for _, res := range shardResults {
			if res == nil {
				return fmt.Errorf("шард не имеет результата")
			}
			totalCount += res.Count
		}
		fmt.Printf("%d\n", totalCount)

	} else {

		for _, res := range shardResults {
			if res == nil {
				return fmt.Errorf("шард не имеет результата")
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
