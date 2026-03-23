package distributed

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/network"
)

// Shard – информация о шарде.
type Shard struct {
	ID               int
	Lines            []string
	StartLineNum     int
	SuccessCount     int
	Result           *models.Result
	OriginalReplicas []string
	UsedWorkers      map[string]bool
	ExpectedResp     int
	mu               sync.Mutex
}

// Coordinator управляет распределённой обработкой.
type Coordinator struct {
	cfg               *configuration.Config
	workers           []string
	client            network.Client
	replicationFactor int
	quorumPerShard    int
	numShards         int
	shards            []*Shard
	shardResults      []*models.Result
}

// New создаёт координатора.
func New(cfg *configuration.Config, workers []string) *Coordinator {
	return &Coordinator{
		cfg:     cfg,
		workers: workers,
		client:  &network.TCPClient{},
	}
}

// Run запускает процесс обработки.
func (c *Coordinator) Run(ctx context.Context, inputReader io.Reader) error {
	// 1. Чтение всех строк
	scanner := bufio.NewScanner(inputReader)
	lines := make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ошибка чтения входных данных: %w", err)
	}
	totalLines := len(lines)
	slog.Info("Прочитано строк", "count", totalLines)

	// 2. Определение параметров из окружения
	c.replicationFactor = 2
	if rfStr := os.Getenv("REPLICATION_FACTOR_MYGOGREP"); rfStr != "" {
		if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
			c.replicationFactor = rf
			if c.replicationFactor > len(c.workers) {
				c.replicationFactor = len(c.workers)
				slog.Warn("Фактор репликации больше числа воркеров, установлен в", "replicationFactor", c.replicationFactor)
			}
		} else {
			slog.Warn("Некорректное значение REPLICATION_FACTOR_MYGOGREP, используется значение по умолчанию", "value", rfStr)
		}
	}
	c.quorumPerShard = c.replicationFactor/2 + 1

	c.numShards = len(c.workers) * 2
	if nsStr := os.Getenv("COUNT_SCHARD_MYGOGREP"); nsStr != "" {
		if ns, err := strconv.Atoi(nsStr); err == nil && ns > 0 {
			c.numShards = ns
		} else {
			slog.Warn("Некорректное значение COUNT_SCHARD_MYGOGREP, используется значение по умолчанию", "value", nsStr)
		}
	}

	// Вычисляем размер шарда
	shardSize := (totalLines + c.numShards - 1) / c.numShards
	if shardSize < 1 {
		shardSize = 1
	}

	// 3. Создаём шарды
	c.shards = make([]*Shard, 0, c.numShards)
	for i := 0; i < c.numShards; i++ {
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
			Lines:            lines[start:end],
			StartLineNum:     start + 1,
			OriginalReplicas: make([]string, 0, c.replicationFactor),
			UsedWorkers:      make(map[string]bool),
		}
		c.shards = append(c.shards, shard)
	}
	c.numShards = len(c.shards)
	slog.Info("Шардирование", "шардов", c.numShards, "размер_шарда", shardSize)

	// 4. Распределяем первоначальные реплики
	for i, shard := range c.shards {
		for j := 0; j < c.replicationFactor; j++ {
			workerIdx := (i + j) % len(c.workers)
			addr := c.workers[workerIdx]
			shard.OriginalReplicas = append(shard.OriginalReplicas, addr)
			shard.UsedWorkers[addr] = true
		}
		shard.ExpectedResp = len(shard.OriginalReplicas)
	}

	// 5. Проверяем доступность воркеров и при необходимости запускаем недостающие
	if err := c.ensureWorkers(ctx); err != nil {
		return fmt.Errorf("не удалось подготовить воркеры: %w", err)
	}

	// 6. Отправляем задачи и обрабатываем результаты
	return c.processTasks(ctx)
}

// ensureWorkers проверяет доступность всех воркеров, запускает недостающие.
func (c *Coordinator) ensureWorkers(ctx context.Context) error {
	// Для простоты проверяем только первый адрес из списка (если он недоступен, запускаем).
	// В полной версии нужно проверить все уникальные адреса.
	// Здесь мы упрощаем: если какой-то воркер не отвечает, запускаем его как дочерний процесс.
	// Для этого нужен путь к исполняемому файлу (os.Args[0]).
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("не удалось определить путь к исполняемому файлу: %w", err)
	}

	for _, addr := range c.workers {
		if !c.isWorkerReachable(addr) {
			slog.Info("Воркер недоступен, запускаем", "addr", addr)
			cmd := exec.CommandContext(ctx, exePath, "--cluster", addr)
			// Можно перенаправить stdout/stderr для отладки, но пока игнорируем.
			if err := cmd.Start(); err != nil {
				return fmt.Errorf("не удалось запустить воркер на %s: %w", addr, err)
			}
			// Даём время на инициализацию
			time.Sleep(500 * time.Millisecond)
			// Повторно проверяем
			if !c.isWorkerReachable(addr) {
				return fmt.Errorf("воркер на %s не запустился", addr)
			}
		}
	}
	return nil
}

// isWorkerReachable проверяет, доступен ли воркер по TCP.
func (c *Coordinator) isWorkerReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// processTasks отправляет задачи и собирает результаты.
func (c *Coordinator) processTasks(ctx context.Context) error {
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	notifyChan := make(chan struct {
		shardID int
		result  *models.Result
	}, len(c.shards)*c.replicationFactor*2)

	var wg sync.WaitGroup

	// Функция отправки задачи на конкретный воркер
	sendTask := func(shard *Shard, addr string) {
		defer wg.Done()
		task := models.Task{
			Lines:        shard.Lines,
			StartLineNum: shard.StartLineNum,
			After:        c.cfg.After,
			Before:       c.cfg.Before,
			Context:      c.cfg.Context,
			Count:        c.cfg.Count,
			IgnoreCase:   c.cfg.IgnoreCase,
			Invert:       c.cfg.Invert,
			Fixed:        c.cfg.Fixed,
			LineNumber:   c.cfg.LineNumber,
			Pattern:      c.cfg.Pattern,
		}
		res, err := c.client.SendTask(workerCtx, addr, task)
		if err != nil {
			slog.Error("Ошибка при обращении к воркеру", "шард", shard.ID, "addr", addr, "error", err)
			res = &models.Result{Error: err.Error()}
		}
		select {
		case notifyChan <- struct {
			shardID int
			result  *models.Result
		}{shard.ID, res}:
		case <-workerCtx.Done():
		}
	}

	// Отправляем первоначальные реплики
	for _, shard := range c.shards {
		for _, addr := range shard.OriginalReplicas {
			wg.Add(1)
			go sendTask(shard, addr)
		}
	}

	// Сбор результатов
	shardCompleted := make([]bool, c.numShards)
	c.shardResults = make([]*models.Result, c.numShards)
	remainingShards := c.numShards

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second)
	defer timeoutCancel()

	for remainingShards > 0 {
		select {
		case notif := <-notifyChan:
			shard := c.shards[notif.shardID]
			shard.mu.Lock()
			if shardCompleted[shard.ID] {
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
			if shard.SuccessCount >= c.quorumPerShard {
				shardCompleted[shard.ID] = true
				remainingShards--
				c.shardResults[shard.ID] = shard.Result
				slog.Info("Шард обработан", "шард", shard.ID, "успешно", shard.SuccessCount)
				shard.mu.Unlock()
				continue
			}
			if shard.ExpectedResp > 0 {
				shard.mu.Unlock()
				continue
			}
			// Все первоначальные реплики ответили, но кворум не достигнут.
			// Находим неиспользованных воркеров.
			availableWorkers := make([]string, 0, len(c.workers))
			for _, w := range c.workers {
				if !shard.UsedWorkers[w] {
					availableWorkers = append(availableWorkers, w)
				}
			}
			if len(availableWorkers) == 0 {
				shard.mu.Unlock()
				return fmt.Errorf("шард %d: не удалось достичь кворума, нет доступных воркеров", shard.ID)
			}
			newReplicas := c.replicationFactor
			if newReplicas > len(availableWorkers) {
				newReplicas = len(availableWorkers)
			}
			newWorkers := availableWorkers[:newReplicas]
			for _, w := range newWorkers {
				shard.UsedWorkers[w] = true
			}
			shard.ExpectedResp += newReplicas
			shard.mu.Unlock()

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

	workerCancel()
	wg.Wait()
	close(notifyChan)

	// Вывод результатов
	if c.cfg.Count {
		total := 0
		for _, res := range c.shardResults {
			if res == nil {
				return fmt.Errorf("шард не имеет результата")
			}
			total += res.Count
		}
		fmt.Printf("%d\n", total)
	} else {
		for _, res := range c.shardResults {
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
