package master

import (
	"sync"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// Shard содержит информацию о части данных и статусе обработки
// (каждый шард представляет собой фрагмент входных строк, который должен быть обработан
// несколькими воркерами для достижения кворума)
type Shard struct {
	ID               int             // порядковый номер шарда (используется для сохранения порядка)
	Lines            []string        // строки, входящие в этот шард
	StartLineNum     int             // глобальный номер первой строки (нужен для флага -n)
	SuccessCount     int             // количество успешных ответов от воркеров для этого шарда
	Result           *models.Result  // первый успешный результат (используется для вывода)
	OriginalReplicas []string        // список воркеров, которым шард был отправлен изначально
	UsedWorkers      map[string]bool // все воркеры, которым когда-либо отправлялся этот шард
	ExpectedResp     int             // количество ответов, которое ещё ожидается (уменьшается по мере получения)
	mu               sync.Mutex      // защищает поля SuccessCount, Result, UsedWorkers и ExpectedResp
}

// createShards разбивает строки на шарды и назначает первоначальные реплики
// Алгоритм:
//   - Вычисляет размер каждого шарда (округляя вверх)
//   - Создаёт шарды, обрезая последний, если строк не хватает
//   - Для каждого шарда назначает replicationFactor воркеров по кругу (i+j % len(workers))
//   - Заполняет OriginalReplicas и UsedWorkers
func createShards(lines []string, workers []string, replicationFactor int, numShards int) []*Shard {

	totalLines := len(lines)

	// размер шарда: округление вверх, чтобы покрыть все строки
	shardSize := (totalLines + numShards - 1) / numShards
	if shardSize < 1 {
		shardSize = 1
	}

	shards := make([]*Shard, 0, numShards)
	for i := 0; i < numShards; i++ {

		start := i * shardSize
		end := start + shardSize

		// последний шард может быть короче
		if end > totalLines {
			end = totalLines
		}

		// если начало вышло за пределы, прерываем (случай, когда строк меньше, чем шардов)
		if start >= totalLines {
			break
		}

		shard := &Shard{
			ID:               i,
			Lines:            lines[start:end],
			StartLineNum:     start + 1,
			OriginalReplicas: make([]string, 0, replicationFactor),
			UsedWorkers:      make(map[string]bool),
		}

		shards = append(shards, shard)
	}

	// распределяем реплики по воркерам: для каждого шарда назначаем replicationFactor воркеров
	// (используем round-robin: (i + j) % len(workers))
	for i, shard := range shards {
		for j := 0; j < replicationFactor; j++ {

			workerIdx := (i + j) % len(workers)
			addr := workers[workerIdx]
			shard.OriginalReplicas = append(shard.OriginalReplicas, addr)
			shard.UsedWorkers[addr] = true
		}

		// ожидаем ответов ровно от стольких воркеров, сколько отправлено
		shard.ExpectedResp = len(shard.OriginalReplicas)
	}

	return shards
}
