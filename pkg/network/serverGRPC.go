package network

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
	proto "github.com/IPampurin/DistributedMyGoGrep/proto"
)

// GRPCServer реализует интерфейс network.Server для запуска gRPC-сервера
// (управляет жизненным циклом gRPC-сервера: создаёт listener, регистрирует сервис,
// запускает обработку запросов и корректно завершает работу по сигналу контекста)
type GRPCServer struct {
	server *grpc.Server // внутренний gRPC-сервер
	addr   string       // адрес, на котором сервер слушает
}

// Start запускает gRPC-сервер на указанном адресе и передаёт входящие запросы в handler
func (s *GRPCServer) Start(ctx context.Context, addr string, handler TaskHandler) error {

	// 1. Создаём TCP-слушатель на указанном адресе
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("ошибка прослушивания адреса %s: %w", addr, err)
	}

	s.addr = addr
	s.server = grpc.NewServer()

	// 2. Регистрируем реализацию сервиса grep
	proto.RegisterGrepServiceServer(s.server, &grpcService{handler: handler})

	// 3. Запускаем сервер в отдельной горутине
	errCh := make(chan error, 1)
	go func() {
		slog.Info("gRPC-сервер запущен", "addr", addr)
		if err := s.server.Serve(lis); err != nil {
			errCh <- fmt.Errorf("ошибка работы gRPC-сервера: %w", err)
		}
		close(errCh)
	}()

	// 4. Ожидаем либо сигнала завершения, либо ошибки сервера
	select {
	case <-ctx.Done():
		// останавливаем сервер
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped := make(chan struct{})
		go func() {
			s.server.GracefulStop() // корректно завершаем текущие запросы
			close(stopped)
		}()
		select {
		case <-stopped:
			slog.Info("gRPC-сервер корректно остановлен", "addr", addr)
			return nil
		case <-shutdownCtx.Done():
			// если graceful shutdown не успел - принудительно останавливаем
			s.server.Stop()
			return fmt.Errorf("graceful shutdown превысил таймаут")
		}
	case err := <-errCh:
		return err
	}
}

// grpcService реализует интерфейс proto.GrepServiceServer
// (оборачивает бизнес-логику (TaskHandler) в gRPC-совместимый обработчик)
type grpcService struct {
	proto.UnimplementedGrepServiceServer
	handler TaskHandler // функция обработки задачи, переданная извне
}

// ProcessTask - реализация RPC-метода, определённого в grep.proto
// (преобразует protobuf-запрос во внутреннюю структуру models.Task,
// вызывает бизнес-логику и возвращает результат в protobuf-формате)
func (s *grpcService) ProcessTask(ctx context.Context, req *proto.Task) (*proto.Result, error) {

	// преобразуем protobuf Task в модель models.Task
	task := models.Task{
		Lines:        req.Lines,
		StartLineNum: int(req.StartLineNum),
		After:        int(req.After),
		Before:       int(req.Before),
		Context:      int(req.Context),
		Count:        req.Count,
		IgnoreCase:   req.IgnoreCase,
		Invert:       req.Invert,
		Fixed:        req.Fixed,
		LineNumber:   req.LineNumber,
		Pattern:      req.Pattern,
	}

	// вызываем переданный обработчик (это worker.Handler())
	result, err := s.handler(ctx, task)
	if err != nil {
		// при ошибке возвращаем protobuf-результат с заполненным полем Error
		return &proto.Result{Error: err.Error()}, nil
	}

	// успешный случай - преобразуем models.Result в protobuf
	return &proto.Result{
		Lines: result.Lines,
		Count: int32(result.Count),
		Error: result.Error,
	}, nil
}
