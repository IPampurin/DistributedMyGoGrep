package master

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/network"
)

// processShards управляет отправкой задач на воркеры и сбором результатов
// (для каждого шарда отправляет задачу на все его реплики (первоначальные и, если нужно, дополнительные);
// ожидает достижения кворума (replicationFactor/2+1 успешных ответов) для каждого шарда, после чего сохраняет
// первый успешный результат; при необходимости (когда первоначальные реплики ответили, но кворум не достигнут)
// перераспределяет задачу на другие, ещё не использованные воркеры)
func processShards(ctx context.Context, shards []*Shard, workers []string, cfg *configuration.Config, client network.Client) ([]*models.Result, error) {

	// дочерний контекст для отмены всех горутин отправки при завершении
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	numShards := len(shards)
	// предполагаем, что у всех шардов одинаковое количество первоначальных реплик
	replicationFactor := len(shards[0].OriginalReplicas)

	// канал для уведомлений о полученных ответах
	notifyChan := make(chan struct {
		shardID int
		result  *models.Result
	}, numShards*replicationFactor*2)

	var wg sync.WaitGroup

	// sendTask отправляет задачу на конкретный воркер, при ошибке создаёт Result с заполненным полем Error
	sendTask := func(shard *Shard, addr string) {
		defer wg.Done()
		task := models.Task{
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

		res, err := client.SendTask(workerCtx, addr, task)
		if err != nil {
			slog.Error("Ошибка при обращении к воркеру", "шард", shard.ID, "addr", addr, "error", err)
			res = &models.Result{Error: err.Error()}
		}

		// отправляем результат в канал, но не блокируемся, если канал уже закрыт или контекст отменён
		select {
		case notifyChan <- struct {
			shardID int
			result  *models.Result
		}{shard.ID, res}:
		case <-workerCtx.Done():
		}
	}

	// отправляем задачи на все первоначальные реплики каждого шарда
	for _, shard := range shards {
		for _, addr := range shard.OriginalReplicas {
			wg.Add(1)
			go sendTask(shard, addr)
		}
	}

	// сбор результатов
	shardCompleted := make([]bool, numShards)         // помечаем шарды, для которых уже достигнут кворум
	shardResults := make([]*models.Result, numShards) // итоговые результаты
	remainingShards := numShards

	// минимальное количество успешных ответов для шарда
	quorum := replicationFactor/2 + 1

	for remainingShards > 0 {
		select {
		case notif := <-notifyChan:

			shard := shards[notif.shardID]
			shard.mu.Lock()

			// если шард уже завершён, игнорируем лишние ответы
			if shardCompleted[shard.ID] {
				shard.mu.Unlock()
				continue
			}

			// успешный ответ - поле Error пустое
			if notif.result.Error == "" {
				shard.SuccessCount++
				if shard.Result == nil {
					// сохраняем первый успешный результат для вывода
					shard.Result = notif.result
				}
			} else {
				slog.Warn("Ошибочный ответ для шарда", "шард", shard.ID, "error", notif.result.Error)
			}

			// уменьшаем счётчик ожидаемых ответов
			shard.ExpectedResp--

			// проверяем, достигнут ли кворум
			if shard.SuccessCount >= quorum {
				shardCompleted[shard.ID] = true
				remainingShards--
				shardResults[shard.ID] = shard.Result
				slog.Info("Шард обработан", "шард", shard.ID, "успешно", shard.SuccessCount)
				shard.mu.Unlock()
				continue
			}

			// если ещё есть ожидаемые ответы (первоначальные реплики), ждём дальше
			if shard.ExpectedResp > 0 {
				shard.mu.Unlock()
				continue
			}

			// если все первоначальные реплики ответили, но кворум не достигнут,
			// начинаем перераспределение - отправляем задачу на другие воркеры
			availableWorkers := make([]string, 0, len(workers))
			for _, w := range workers {
				if !shard.UsedWorkers[w] {
					availableWorkers = append(availableWorkers, w)
				}
			}

			if len(availableWorkers) == 0 {
				// нет свободных воркеров - обработка шарда невозможна
				shard.mu.Unlock()
				return nil, fmt.Errorf("шард %d: не удалось достичь кворума, нет доступных воркеров", shard.ID)
			}

			// берём replicationFactor новых воркеров (или меньше, если доступных меньше)
			newReplicas := replicationFactor
			if newReplicas > len(availableWorkers) {
				newReplicas = len(availableWorkers)
			}

			newWorkers := availableWorkers[:newReplicas]
			for _, w := range newWorkers {
				shard.UsedWorkers[w] = true
			}

			// увеличиваем счётчик ожидаемых ответов на количество новых реплик
			shard.ExpectedResp += newReplicas
			shard.mu.Unlock()

			// запускаем отправку на новые воркеры
			for _, addr := range newWorkers {
				wg.Add(1)
				go sendTask(shard, addr)
			}

			slog.Info("Перераспределение шарда", "шард", shard.ID, "новые_воркеры", newWorkers)

		case <-workerCtx.Done():

			workerCancel()
			wg.Wait()
			return nil, fmt.Errorf("обработка прервана")
		}
	}

	workerCancel()
	wg.Wait()
	close(notifyChan)

	return shardResults, nil
}
