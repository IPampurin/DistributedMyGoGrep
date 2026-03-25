package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	proto "github.com/IPampurin/DistributedMyGoGrep/proto"
)

// GRPCClient реализует интерфейс network.Client для отправки задач по gRPC
// (клиент кеширует соединения к разным адресам, чтобы не создавать их заново при каждом вызове)
type GRPCClient struct {
	mu      sync.RWMutex                // защищает доступ к карте соединений
	conns   map[string]*grpc.ClientConn // кеш gRPC-соединений по адресу
	timeout time.Duration               // таймаут для отдельных вызовов ProcessTask
}

// NewGRPCClient создаёт новый экземпляр gRPC-клиента с пустым кешем соединений
func NewGRPCClient() *GRPCClient {

	return &GRPCClient{
		conns:   make(map[string]*grpc.ClientConn),
		timeout: 30 * time.Second,
	}
}

// getConnection возвращает существующее соединение к указанному адресу, либо создаёт новое
// (реализована двойная проверка с блокировками для потокобезопасности)
func (c *GRPCClient) getConnection(addr string) (*grpc.ClientConn, error) {

	// быстрая проверка без блокировки на запись
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return conn, nil
	}

	// блокируем для записи, чтобы гарантировать создание только одного соединения
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	// создаём соединение через grpc.NewClient
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания соединения к %s: %w", addr, err)
	}

	// инициируем подключение
	conn.Connect()

	// ожидаем, пока соединение не станет Ready
	state := conn.GetState()
	for state != connectivity.Ready {
		if !conn.WaitForStateChange(ctx, state) {
			conn.Close()
			return nil, fmt.Errorf("соединение с %s не стало Ready в течение таймаута", addr)
		}
		state = conn.GetState()
	}

	c.conns[addr] = conn

	return conn, nil
}

// SendTask отправляет задачу на указанный адрес через gRPC
// (использует кешированное соединение, преобразует models.Task в protobuf-запрос, вызывает ProcessTask и возвращает результат)
func (c *GRPCClient) SendTask(ctx context.Context, addr string, task models.Task) (*models.Result, error) {

	conn, err := c.getConnection(addr)
	if err != nil {
		return nil, fmt.Errorf("не удалось установить gRPC-соединение: %w", err)
	}

	client := proto.NewGrepServiceClient(conn)

	// преобразуем в protobuf-структуру
	req := &proto.Task{
		Lines:        task.Lines,
		StartLineNum: int32(task.StartLineNum),
		After:        int32(task.After),
		Before:       int32(task.Before),
		Context:      int32(task.Context),
		Count:        task.Count,
		IgnoreCase:   task.IgnoreCase,
		Invert:       task.Invert,
		Fixed:        task.Fixed,
		LineNumber:   task.LineNumber,
		Pattern:      task.Pattern,
	}

	// если в контексте нет дедлайна, устанавливаем свой таймаут
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	// вызов gRPC-метода
	resp, err := client.ProcessTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения gRPC-запроса: %w", err)
	}

	// преобразуем protobuf-ответ в models.Result
	return &models.Result{
		Lines: resp.Lines,
		Count: int(resp.Count),
		Error: resp.Error,
	}, nil
}

// Close закрывает все кешированные gRPC-соединения
// (должен вызываться при завершении работы клиента)
func (c *GRPCClient) Close() error {

	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("ошибка закрытия соединения к %s: %w", addr, err)
		}
	}

	return nil
}
