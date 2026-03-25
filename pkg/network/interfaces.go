package network

import (
	"context"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// Client определяет интерфейс для отправки задачи воркеру
type Client interface {
	SendTask(ctx context.Context, addr string, task models.Task) (*models.Result, error)
}

// Server определяет интерфейс для запуска сервера, обрабатывающего задачи
type Server interface {
	Start(ctx context.Context, addr string, handler TaskHandler) error
}

// TaskHandler обрабатывает задачу и возвращает результат
type TaskHandler func(ctx context.Context, task models.Task) (*models.Result, error)
