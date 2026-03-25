package master

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
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/network"
)

// Master управляет распределённой обработкой
type Master struct {
	cfg               *configuration.Config
	workers           []string
	client            network.Client
	replicationFactor int
	numShards         int
}

// New создаёт координатора
func New(cfg *configuration.Config) *Master {
	return &Master{
		cfg:     cfg,
		workers: cfg.SrvAddrs,
		client:  network.NewHTTPClient(),
	}
}

// Run запускает процесс обработки
func (m *Master) Run(ctx context.Context, inputReader io.Reader) error {

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
	m.replicationFactor = 2
	if rfStr := os.Getenv("REPLICATION_FACTOR_MYGOGREP"); rfStr != "" {
		if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
			m.replicationFactor = rf
			if m.replicationFactor > len(m.workers) {
				m.replicationFactor = len(m.workers)
				slog.Warn("Фактор репликации больше числа воркеров, установлен в", "replicationFactor", m.replicationFactor)
			}
		} else {
			slog.Warn("Некорректное значение REPLICATION_FACTOR_MYGOGREP, используется значение по умолчанию", "value", rfStr)
		}
	}

	m.numShards = len(m.workers) * 2
	if nsStr := os.Getenv("COUNT_SCHARD_MYGOGREP"); nsStr != "" {
		if ns, err := strconv.Atoi(nsStr); err == nil && ns > 0 {
			m.numShards = ns
		} else {
			slog.Warn("Некорректное значение COUNT_SCHARD_MYGOGREP, используется значение по умолчанию", "value", nsStr)
		}
	}

	// 3. Создаём шарды
	shards := createShards(lines, m.workers, m.replicationFactor, m.numShards)
	if len(shards) == 0 {
		return fmt.Errorf("не удалось создать шарды")
	}
	slog.Info("Шардирование", "шардов", len(shards))

	// 4. Проверяем доступность воркеров и при необходимости запускаем недостающие
	if err := m.ensureWorkers(ctx); err != nil {
		return fmt.Errorf("не удалось подготовить воркеры: %w", err)
	}

	// 5. Отправляем задачи и собираем результаты
	results, err := processShards(ctx, shards, m.workers, m.cfg, m.client)
	if err != nil {
		return err
	}

	// 6. Выводим результат
	return printResults(m.cfg, results)
}

// ensureWorkers проверяет доступность всех воркеров, запускает недостающие
func (m *Master) ensureWorkers(ctx context.Context) error {

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("не удалось определить путь к исполняемому файлу: %w", err)
	}

	for _, addr := range m.workers {
		if !m.isWorkerReachable(addr) {
			slog.Info("Воркер недоступен, запускаем", "addr", addr)
			cmd := exec.CommandContext(ctx, exePath, "--addr", addr)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				return fmt.Errorf("не удалось запустить воркер на %s: %w", addr, err)
			}
			// даём время на инициализацию (isWorkerReachable уже имеет ретраи)
			if !m.isWorkerReachable(addr) {
				return fmt.Errorf("воркер на %s не запустился", addr)
			}
		}
	}

	return nil
}

// isWorkerReachable проверяет доступность воркера по TCP с ретраями
func (m *Master) isWorkerReachable(addr string) bool {

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		// экспоненциальная задержка: 100ms, 200ms, 400ms
		time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
	}

	return false
}
