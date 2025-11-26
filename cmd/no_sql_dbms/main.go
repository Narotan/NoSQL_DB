package main

import (
	"encoding/json"
	"fmt"
	"log"
	"nosql_db/internal/index"
	"nosql_db/internal/operators"
	"nosql_db/internal/query"
	"nosql_db/internal/storage"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		log.Fatal()
	}

	dbName := os.Args[1]
	command := os.Args[2]

	var jsonQuery string
	if len(os.Args) >= 4 {
		jsonQuery = os.Args[3]
	}

	if err := executeCommand(dbName, command, jsonQuery); err != nil {
		log.Fatal(err)
	}
}

func executeCommand(dbName, command, jsonQuery string) error {
	switch command {
	case "insert":
		return cmdInsert(dbName, jsonQuery)
	case "find":
		return cmdFind(dbName, jsonQuery)
	case "delete":
		return cmdDelete(dbName, jsonQuery)
	case "create_index":
		return cmdCreateIndex(dbName, jsonQuery)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func cmdInsert(dbName, jsonDoc string) error {
	if jsonDoc == "" {
		return fmt.Errorf("insert requires a JSON document")
	}

	doc, err := query.ParseDocument(jsonDoc)
	if err != nil {
		return err
	}

	coll, err := storage.LoadCollection(dbName)
	if err != nil {
		return fmt.Errorf("failed to load collection: %w", err)
	}

	// Загружаем индексы перед вставкой
	if err := coll.LoadAllIndexes(); err != nil {
		return fmt.Errorf("failed to load indexes: %w", err)
	}

	id, err := coll.Insert(doc)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	if err := coll.Save(); err != nil {
		return fmt.Errorf("failed to save collection: %w", err)
	}

	// Сохраняем все индексы после вставки
	if err := coll.SaveAllIndexes(); err != nil {
		return fmt.Errorf("failed to save indexes: %w", err)
	}

	fmt.Printf("Document inserted successfully. ID: %s\n", id)
	return nil
}

func cmdFind(dbName, jsonQuery string) error {
	q, err := query.Parse(jsonQuery)
	if err != nil {
		return err
	}

	coll, err := storage.LoadCollection(dbName)
	if err != nil {
		return fmt.Errorf("failed to load collection: %w", err)
	}

	// Загружаем все индексы
	if err := coll.LoadAllIndexes(); err != nil {
		return fmt.Errorf("failed to load indexes: %w", err)
	}

	var results []map[string]any

	// Пробуем использовать индекс для простого запроса с одним условием
	if len(q.Conditions) == 1 && !hasLogicalOperators(q.Conditions) {
		for field, condition := range q.Conditions {
			if coll.HasIndex(field) {
				// Используем индекс!
				results = findWithIndex(coll, field, condition)
				break
			}
		}
	}

	// Если индекс не использован - делаем full scan
	if results == nil {
		results = findFullScan(coll, q)
	}

	if len(results) == 0 {
		fmt.Println("[]")
		return nil
	}

	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

// hasLogicalOperators проверяет наличие $or или $and в условиях
func hasLogicalOperators(conditions map[string]any) bool {
	_, hasOr := conditions["$or"]
	_, hasAnd := conditions["$and"]
	return hasOr || hasAnd
}

// findWithIndex выполняет поиск используя индекс
func findWithIndex(coll *storage.Collection, field string, condition any) []map[string]any {
	btree, ok := coll.GetIndex(field)
	if !ok {
		return nil
	}

	var docIDs []string

	// Простое условие: {"age": 25} - неявный $eq
	switch v := condition.(type) {
	case float64, int, int64, string, bool:
		// Прямое значение - точечный поиск
		key := index.ValueToKey(v)
		values := btree.Search(key)
		docIDs = index.ValuesToStrings(values)

	case map[string]any:
		// Условие с операторами: {"age": {"$gt": 20}}
		if gtValue, exists := v["$gt"]; exists {
			key := index.ValueToKey(gtValue)
			values := btree.SearchGreaterThan(key)
			docIDs = index.ValuesToStrings(values)
		} else if ltValue, exists := v["$lt"]; exists {
			key := index.ValueToKey(ltValue)
			values := btree.SearchLessThan(key)
			docIDs = index.ValuesToStrings(values)
		} else if eqValue, exists := v["$eq"]; exists {
			key := index.ValueToKey(eqValue)
			values := btree.Search(key)
			docIDs = index.ValuesToStrings(values)
		} else if inValues, exists := v["$in"]; exists {
			// Множественный точечный поиск
			if inArray, ok := inValues.([]any); ok {
				var keys []index.Key
				for _, val := range inArray {
					keys = append(keys, index.ValueToKey(val))
				}
				values := btree.SearchIn(keys)
				docIDs = index.ValuesToStrings(values)
			}
		}
	}

	// Получаем документы по _id из HashMap
	var results []map[string]any
	for _, id := range docIDs {
		if doc, ok := coll.GetByID(id); ok {
			results = append(results, doc)
		}
	}

	return results
}

// findFullScan выполняет полное сканирование коллекции
func findFullScan(coll *storage.Collection, q *query.Query) []map[string]any {
	var results []map[string]any

	allDocs := coll.All()
	for _, doc := range allDocs {
		if operators.MatchDocument(doc, q.Conditions) {
			results = append(results, doc)
		}
	}

	return results
}

func cmdDelete(dbName, jsonQuery string) error {
	q, err := query.Parse(jsonQuery)
	if err != nil {
		return err
	}

	coll, err := storage.LoadCollection(dbName)
	if err != nil {
		return fmt.Errorf("failed to load collection: %w", err)
	}

	// Загружаем индексы перед удалением
	if err := coll.LoadAllIndexes(); err != nil {
		return fmt.Errorf("failed to load indexes: %w", err)
	}

	allDocs := coll.All()
	deletedCount := 0

	for _, doc := range allDocs {
		if operators.MatchDocument(doc, q.Conditions) {
			// Получаем _id и удаляем
			if id, ok := doc["_id"].(string); ok {
				if coll.Delete(id) {
					deletedCount++
				}
			}
		}
	}

	if err := coll.Save(); err != nil {
		return fmt.Errorf("failed to save collection: %w", err)
	}

	// После удаления пересоздаем все индексы (упрощенный подход)
	if err := coll.RebuildAllIndexes(); err != nil {
		return fmt.Errorf("failed to rebuild indexes: %w", err)
	}

	fmt.Printf("Deleted %d document(s).\n", deletedCount)
	return nil
}

func cmdCreateIndex(dbName, fieldName string) error {
	// Загружаем коллекцию
	coll, err := storage.LoadCollection(dbName)
	if err != nil {
		return fmt.Errorf("failed to load collection: %w", err)
	}

	// Создаем индекс (по умолчанию order = 64)
	if err := coll.CreateIndex(fieldName, 64); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	fmt.Printf("Index created successfully on field '%s'.\n", fieldName)
	return nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  no_sql_dbms <database_name> insert '<json_document>'")
	fmt.Println("  no_sql_dbms <database_name> find '<json_query>'")
	fmt.Println("  no_sql_dbms <database_name> delete '<json_query>'")
	fmt.Println("  no_sql_dbms <database_name> create_index <field_name>")
	fmt.Println("\nExamples:")
	fmt.Println(`  no_sql_dbms my_database insert '{"name": "Alice", "age": 25, "city": "London"}'`)
	fmt.Println(`  no_sql_dbms my_database find '{"age": 25}'`)
	fmt.Println(`  no_sql_dbms my_database find '{"age": {"$gt": 20}}'`)
	fmt.Println(`  no_sql_dbms my_database delete '{"name": {"$like": "A%"}}'`)
}
