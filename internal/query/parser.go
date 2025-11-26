package query

import (
	"encoding/json"
	"fmt"
)

// Parse парсит json-строку запроса в структуру Query
func Parse(jsonStr string) (*Query, error) {
	if jsonStr == "" {
		return &Query{
			Conditions: make(map[string]any),
		}, nil
	}

	var conditions map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &conditions); err != nil {
		return nil, fmt.Errorf("invalid JSON query: %w", err)
	}

	return &Query{Conditions: conditions}, nil
}

// ParseDocument парсит json-строку документа
func ParseDocument(jsonStr string) (map[string]any, error) {
	var doc map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return nil, fmt.Errorf("invalid JSON document: %w", err)
	}
	return doc, nil
}
