package configuration

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

// режимы работы программы
const (
	ModeLocal  = "local"  // обычный grep (без сети)
	ModeNode   = "node"   // режим сервера (запуск одного или нескольких узлов кластера)
	ModeClient = "client" // режим клиента (отправка задания на кластер)
)

// Config хранит все параметры командной строки
type Config struct {
	After      int      // -A печатать +N строк после совпадения
	Before     int      // -B печатать +N строк до совпадения
	Context    int      // -C печатать ±N строк вокруг совпадения
	Count      bool     // -c вывести только количество совпадений
	IgnoreCase bool     // -i игнорировать регистр
	Invert     bool     // -v инвертировать вывод
	Fixed      bool     // -F считать шаблон фиксированной строкой
	LineNumber bool     // -n печатать номер строки
	Pattern    string   // шаблон поиска (обязателен для local и client)
	Filename   string   // имя файла (если пусто - читать из stdin)
	SrvAddrs   []string // список адресов кластера
	Mode       string   // вычисленный режим работы
}

// примеры запуска:
// локальный режим - ./mygogrep -i banana test1.txt
// режим ноды      - ./mygogrep --addr localhost:9090,localhost:9091
// режим клиента   - ./mygogrep --cluster localhost:9090,localhost:9091,localhost:9092 -i banana test1.txt

// ParseFlags обрабатывает аргументы командной строки и заполняет Config
func ParseFlags() (*Config, error) {

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

	// флаги распределённого режима
	var addrFlag, clusterFlag string
	flag.StringVar(&addrFlag, "addr", "", "Режим ноды: список адресов узлов через запятую (например, localhost:9090,localhost:9091)")
	flag.StringVar(&clusterFlag, "cluster", "", "Режим клиента: список адресов кластера через запятую")

	// настраиваем вывод помощи
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Использование: %s [опции] [режим]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Режимы:\n")
		fmt.Fprintf(os.Stderr, "  1. Локальный grep: %s [флаги grep] шаблон [файл]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "     Пример: %s -i banana test1.txt\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  2. Режим ноды: %s --addr адрес1,адрес2,...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "     Пример: %s --addr localhost:9090,localhost:9091\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  3. Режим клиента: %s --cluster адрес1,адрес2,... [флаги grep] шаблон [файл]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "     Пример: %s --cluster localhost:9090,localhost:9091,localhost:9092 -i banana test1.txt\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Флаги: ")
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()

	// определяем режим работы
	switch {
	case addrFlag != "": // режим ноды

		cfg.Mode = ModeNode
		addrs, err := parseClusterAddrs(addrFlag)
		if err != nil {
			return nil, fmt.Errorf("ошибка в --addr: %v", err)
		}
		if err := validateAddrs(addrs); err != nil {
			return nil, fmt.Errorf("ошибка в --addr: %v", err)
		}
		cfg.SrvAddrs = addrs

		return cfg, nil // в режиме ноды шаблон и имя файла не используются, игнорируем переданные аргументы

	case clusterFlag != "": // режим клиента

		cfg.Mode = ModeClient
		addrs, err := parseClusterAddrs(clusterFlag)
		if err != nil {
			return nil, fmt.Errorf("ошибка в --cluster: %v", err)
		}
		if err := validateAddrs(addrs); err != nil {
			return nil, fmt.Errorf("ошибка в --cluster: %v", err)
		}
		cfg.SrvAddrs = addrs
		if len(args) < 1 {
			return nil, fmt.Errorf("в режиме клиента необходимо указать шаблон поиска")
		}
		cfg.Pattern = args[0]
		if len(args) >= 2 {
			cfg.Filename = args[1]
		} else {
			cfg.Filename = "" // пустая строка означает чтение из stdin
		}
		// обработка флага -C
		if cfg.Context > 0 {
			cfg.After = cfg.Context
			cfg.Before = cfg.Context
		}

		return cfg, nil

	default:

		// локальный режим
		cfg.Mode = ModeLocal
		if len(args) < 1 {
			return nil, fmt.Errorf("в локальном режиме необходимо указать шаблон поиска")
		}
		cfg.Pattern = args[0]
		if len(args) >= 2 {
			cfg.Filename = args[1]
		} else {
			cfg.Filename = "" // пустая строка означает чтение из stdin
		}
		// обработка флага -C
		if cfg.Context > 0 {
			cfg.After = cfg.Context
			cfg.Before = cfg.Context
		}

		return cfg, nil
	}
}

// parseClusterAddrs разбивает строку с адресами на слайс, удаляя пробелы и пустые элементы
func parseClusterAddrs(s string) ([]string, error) {

	if s == "" {
		return nil, fmt.Errorf("список адресов не может быть пустым")
	}

	parts := strings.Split(s, ",")

	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		addrs = append(addrs, trimmed)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("не найдено ни одного корректного адреса")
	}

	return addrs, nil
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
