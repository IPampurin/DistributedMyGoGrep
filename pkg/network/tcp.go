package network

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// TCPClient реализует Client по протоколу TCP/JSON.
type TCPClient struct{}

func (c *TCPClient) SendTask(ctx context.Context, addr string, task models.Task) (*models.Result, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("не удалось подключиться к %s: %w", addr, err)
	}
	defer conn.Close()

	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("ошибка сериализации задачи: %w", err)
	}

	// отправляем длину (4 байта, big endian)
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

	// читаем ответ
	lenBufResp := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBufResp); err != nil {
		return nil, fmt.Errorf("ошибка чтения длины ответа: %w", err)
	}
	respLen := int(uint32(lenBufResp[0])<<24 | uint32(lenBufResp[1])<<16 | uint32(lenBufResp[2])<<8 | uint32(lenBufResp[3]))
	if respLen <= 0 || respLen > 10*1024*1024 {
		return nil, fmt.Errorf("неверная длина ответа: %d", respLen)
	}
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respData); err != nil {
		return nil, fmt.Errorf("ошибка чтения ответа: %w", err)
	}
	var result models.Result
	if err := json.Unmarshal(respData, &result); err != nil {
		return nil, fmt.Errorf("ошибка разбора JSON ответа: %w", err)
	}
	if result.Error != "" {
		return &result, fmt.Errorf("воркер вернул ошибку: %s", result.Error)
	}
	return &result, nil
}

// TCPServer реализует Server.
type TCPServer struct {
	handler TaskHandler
}

func (s *TCPServer) Start(ctx context.Context, addr string, handler TaskHandler) error {
	s.handler = handler // <-- ИСПРАВЛЕНО: сохраняем обработчик

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("не удалось запустить слушатель на %s: %w", addr, err)
	}
	defer listener.Close()

	// запускаем горутину для приёма соединений
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					slog.Error("Ошибка при приёме соединения", "error", err)
					continue
				}
			}
			go s.handleConnection(ctx, conn)
		}
	}()

	// ждём отмены контекста
	<-ctx.Done()
	slog.Info("Остановка сервера", "addr", addr)
	return nil
}

func (s *TCPServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	// читаем задачу
	task, err := readTask(conn)
	if err != nil {
		slog.Error("Ошибка чтения задачи", "error", err)
		sendResult(conn, models.Result{Error: err.Error()})
		return
	}
	// обрабатываем
	res, err := s.handler(*task) // здесь handler уже не nil
	if err != nil {
		sendResult(conn, models.Result{Error: err.Error()})
		return
	}
	sendResult(conn, *res)
}

// readTask читает из соединения задачу (длина + JSON).
func readTask(conn net.Conn) (*models.Task, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("ошибка чтения длины сообщения: %w", err)
	}
	msgLen := int(uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3]))
	if msgLen <= 0 || msgLen > 10*1024*1024 {
		return nil, fmt.Errorf("неверная длина сообщения: %d", msgLen)
	}
	msg := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msg); err != nil {
		return nil, fmt.Errorf("ошибка чтения тела сообщения: %w", err)
	}
	var task models.Task
	if err := json.Unmarshal(msg, &task); err != nil {
		return nil, fmt.Errorf("ошибка разбора JSON: %w", err)
	}
	return &task, nil
}

// sendResult отправляет результат в соединение (длина + JSON).
func sendResult(conn net.Conn, res models.Result) error {
	data, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("ошибка сериализации результата: %w", err)
	}
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))
	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("ошибка отправки длины: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("ошибка отправки данных: %w", err)
	}
	return nil
}
