package network

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// HTTPClient реализует интерфейс Client для отправки задач по HTTP
type HTTPClient struct {
	httpClient *http.Client
}

// NewHTTPClient создаёт новый HTTPClient с таймаутом по умолчанию
func NewHTTPClient() *HTTPClient {
	return &HTTPClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SendTask отправляет задачу на указанный адрес (http://addr/task) и возвращает результат
func (c *HTTPClient) SendTask(ctx context.Context, addr string, task models.Task) (*models.Result, error) {

	// сериализуем задачу в JSON
	body, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("ошибка сериализации задачи: %w", err)
	}

	// создаём HTTP-запрос с контекстом
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/task", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// выполняем запрос
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса к %s: %w", addr, err)
	}
	defer resp.Body.Close()

	// проверяем статус
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("воркер вернул ошибку: %s", resp.Status)
	}

	// читаем тело ответа
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	// декодируем ответ
	var result models.Result
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("ошибка декодирования ответа: %w", err)
	}

	return &result, nil
}
