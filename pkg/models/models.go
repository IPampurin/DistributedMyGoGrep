package models

import "sync"

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

// GrepResult содержит результат обработки (строки или количество)
type GrepResult struct {
	Lines   []string // строки для вывода (с учётом флагов -n и т.д.)
	Count   int      // количество совпадений (если флаг -c)
	IsCount bool     // true если результат - это счётчик
}
