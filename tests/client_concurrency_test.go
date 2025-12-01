package main_test

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
)

type apiRequest struct {
	Database string           `json:"database"`
	Command  string           `json:"operation"`
	Data     []map[string]any `json:"data,omitempty"`
	Query    map[string]any   `json:"query,omitempty"`
}

type apiResponse struct {
	Status  string           `json:"status"`
	Message string           `json:"message"`
	Data    []map[string]any `json:"data,omitempty"`
	Count   int              `json:"count,omitempty"`
}

func sendRequest(t *testing.T, req apiRequest) apiResponse {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(req); err != nil {
		t.Fatalf("Encode error: %v", err)
	}

	var resp apiResponse
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	return resp
}

func TestConcurrentInsertAndFind(t *testing.T) {
	coll := "users"
	wg := sync.WaitGroup{}
	N := 20

	// Очищаем коллекцию
	sendRequest(t, apiRequest{
		Database: coll,
		Command:  "delete",
		Query:    map[string]any{},
	})

	// Параллельные вставки
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := sendRequest(t, apiRequest{
				Database: coll,
				Command:  "insert",
				Data:     []map[string]any{{"name": fmt.Sprintf("user%d", i), "age": 20 + i}},
			})
			if resp.Status != "success" {
				t.Errorf("Insert failed: %v", resp.Message)
			}
		}(i)
	}
	wg.Wait()

	// Проверяем количество
	resp := sendRequest(t, apiRequest{
		Database: coll,
		Command:  "find",
		Query:    map[string]any{},
	})
	if resp.Count != N {
		t.Errorf("Expected %d docs, got %d", N, resp.Count)
	}
}

func TestConcurrentDeleteAndInsert(t *testing.T) {
	coll := "users"
	wg := sync.WaitGroup{}
	N := 10

	// Заполняем коллекцию
	for i := 0; i < N; i++ {
		sendRequest(t, apiRequest{
			Database: coll,
			Command:  "insert",
			Data:     []map[string]any{{"name": fmt.Sprintf("deluser%d", i)}},
		})
	}

	// Параллельно удаляем и вставляем
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			sendRequest(t, apiRequest{
				Database: coll,
				Command:  "delete",
				Query:    map[string]any{"name": fmt.Sprintf("deluser%d", i)},
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			sendRequest(t, apiRequest{
				Database: coll,
				Command:  "insert",
				Data:     []map[string]any{{"name": fmt.Sprintf("newuser%d", i)}},
			})
		}(i)
	}
	wg.Wait()

	// Проверяем, что хотя бы N новых пользователей есть
	resp := sendRequest(t, apiRequest{
		Database: coll,
		Command:  "find",
		Query:    map[string]any{"name": map[string]any{"$like": "newuser%"}},
	})
	if resp.Count < N {
		t.Errorf("Expected at least %d new users, got %d", N, resp.Count)
	}
}

func TestConcurrentIndexing(t *testing.T) {
	coll := "users"
	wg := sync.WaitGroup{}
	N := 5

	// Параллельно создаём индексы
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			field := fmt.Sprintf("field%d", i)
			sendRequest(t, apiRequest{
				Database: coll,
				Command:  "create_index",
				Query:    map[string]any{field: nil},
			})
		}(i)
	}
	wg.Wait()
}
