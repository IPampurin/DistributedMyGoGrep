package configuration

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

// Config хранит все параметры командной строки
type Config struct {
	After        int      // -A
	Before       int      // -B
	Context      int      // -C
	Count        bool     // -c
	IgnoreCase   bool     // -i
	Invert       bool     // -v
	Fixed        bool     // -F
	LineNumber   bool     // -n
	Pattern      string   // шаблон (обязательный аргумент)
	Filename     string   // имя файла (необязательный аргумент)
	ClusterAddrs []string // --cluster "host:port,host:port,host:port"
	/*
		- нет адресов - работа локально как с обычной утилитой
		- один адрес (и хост, и порт обязательны) - запуск экземпляра для возможного приёма потока данных (эта нода может быть указана в списке серверов для обработки)
		- более одного адреса сервера - считаем это списком нод распределённого режима
	*/
}

// ParseFlags обрабатывает аргументы командной строки и заполняет Config
// (возвращает конфигурацию и оставшиеся аргументы после флагов)
func ParseFlags() (*Config, []string, error) {

	cfg := &Config{}

	// определяем флаги grep
	flag.IntVar(&cfg.After, "A", 0, "Вывести N строк после совпадения")
	flag.IntVar(&cfg.Before, "B", 0, "Вывести N строк до совпадения")
	flag.IntVar(&cfg.Context, "C", 0, "Вывести N строк контекста вокруг совпадения (переопределяет -A и -B)")
	flag.BoolVar(&cfg.Count, "c", false, "Вывести только количество совпадающих строк")
	flag.BoolVar(&cfg.IgnoreCase, "i", false, "Игнорировать регистр")
	flag.BoolVar(&cfg.Invert, "v", false, "Инвертировать фильтр (выводить строки, не содержащие шаблон)")
	flag.BoolVar(&cfg.Fixed, "F", false, "Трактовать шаблон как фиксированную строку, а не регулярное выражение")
	flag.BoolVar(&cfg.LineNumber, "n", false, "Выводить номер строки перед каждой найденной строкой")

	// флаг распределённого режима
	var clusterRaw string
	flag.StringVar(&clusterRaw, "cluster", "", "Список адресов узлов через запятую (например, localhost:9090,localhost:9091). Если не указан — работаем локально. Если указан ровно один — запускаем ноду для приёма задач. Если больше — координатор.")

	// настраиваем вывод помощи
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Использование: %s [флаги] шаблон [файл]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Флаги:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// парсим адреса кластера
	cfg.ClusterAddrs = parseClusterAddrs(clusterRaw)

	// валидируем адреса (если они заданы)
	if err := validateAddrs(cfg.ClusterAddrs); err != nil {
		return nil, nil, fmt.Errorf("неверный формат адресов в --cluster: %w", err)
	}

	// после парсинга флагов остаются позиционные аргументы: шаблон и (опционально) файл
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	cfg.Pattern = args[0]
	if len(args) > 1 {
		cfg.Filename = args[1]
	}

	// обработка контекста: если указан -C, он переопределяет -A и -B
	if cfg.Context > 0 {
		cfg.After = cfg.Context
		cfg.Before = cfg.Context
	}

	return cfg, args[2:], nil // возвращаем конфиг и оставшиеся аргументы (на случай расширения)
}

// parseClusterAddrs разбивает строку с адресами на слайс, удаляя пробелы и пустые элементы
func parseClusterAddrs(raw string) []string {

	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")

	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		result = append(result, p)
	}

	return result
}

// validateAddrs проверяет, что все адреса имеют формат host:port
func validateAddrs(addrs []string) error {

	for _, addr := range addrs {
		_, _, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("адрес %q: %w", addr, err)
		}
	}

	return nil
}
