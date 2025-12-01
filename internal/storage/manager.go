package storage

import (
	"fmt"
	"sync"
)

// WriteJob — задача в очереди модификации
type WriteJob struct {
	DBName     string                                      // имя базы/коллекции
	Operation  func(coll *Collection) (WriteResult, error) // операция для выполнения
	ResultChan chan WriteResult                            // канал для ответа
}

// WriteResult — результат выполнения write-операции
type WriteResult struct {
	InsertedIDs  []string // ID вставленных документов
	DeletedCount int      // количество удаленных документов
	Message      string   // сообщение
	Error        error    // ошибка, если есть
}

type CollectionMng struct {
	mu          sync.Mutex
	collections map[string]*Collection
	writeQueue  chan WriteJob
	stopChan    chan struct{}
}

const writeQueueSize = 100

func NewManager() *CollectionMng {
	m := &CollectionMng{
		collections: make(map[string]*Collection),
		writeQueue:  make(chan WriteJob, writeQueueSize),
		stopChan:    make(chan struct{}),
	}
	go m.worker()
	return m
}

var GlobalManager = NewManager()

func (m *CollectionMng) GetCollection(name string) (*Collection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if coll, exist := m.collections[name]; exist {
		return coll, nil
	}

	coll, err := LoadCollection(name)
	if err != nil {
		return nil, err
	}

	if err := coll.LoadAllIndexes(); err != nil {
		return nil, fmt.Errorf("failed to load index %w", err)
	}

	m.collections[name] = coll

	return coll, nil
}

func (m *CollectionMng) worker() {
	for {
		select {
		case job := <-m.writeQueue:
			result := m.processJob(job)
			job.ResultChan <- result
		case <-m.stopChan:
			return
		}
	}
}

func (m *CollectionMng) processJob(job WriteJob) WriteResult {
	coll, err := m.GetCollection(job.DBName)
	if err != nil {
		return WriteResult{Error: fmt.Errorf("failed to get collection: %w", err)}
	}

	result, err := job.Operation(coll)
	if err != nil {
		return WriteResult{Error: err}
	}
	return result
}

func (m *CollectionMng) Enqueue(dbName string, operation func(coll *Collection) (WriteResult, error)) WriteResult {
	resultChan := make(chan WriteResult, 1)
	job := WriteJob{
		DBName:     dbName,
		Operation:  operation,
		ResultChan: resultChan,
	}
	m.writeQueue <- job
	return <-resultChan
}

func (m *CollectionMng) Stop() {
	close(m.stopChan)
}
