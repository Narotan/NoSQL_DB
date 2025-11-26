package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"nosql_db/internal/index"
	"os"
	"path/filepath"
	"time"
)

type Collection struct {
	Name    string
	Data    *HashMap
	Indexes map[string]*index.BTree
}

func NewCollection(name string) *Collection {
	return &Collection{
		Name:    name,
		Data:    NewHashMap(),
		Indexes: make(map[string]*index.BTree),
	}
}

func generateID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(1000000))
}

// loadCollection загружает коллекцию из базы данных
func LoadCollection(name string) (*Collection, error) {
	path := filepath.Join("data", name+".json")

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return NewCollection(name), nil
	}

	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(bytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	hmap := NewHashMap()
	for k, v := range raw {
		hmap.Put(k, v)
	}

	coll := NewCollection(name)
	coll.Data = hmap
	return coll, nil
}

// save сохраняет данные в json в базе данных
func (c *Collection) Save() error {
	items := c.Data.Items()
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("mkdir error: %w", err)
	}

	path := filepath.Join("data", c.Name+".json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write file error: %w", err)
	}

	return nil
}

func (c *Collection) Insert(doc map[string]any) (string, error) {
	id := generateID()
	doc["_id"] = id
	c.Data.Put(id, doc)

	// автоматически обновляем все индексы
	c.updateIndexesOnInsert(id, doc)

	return id, nil
}

// updateIndexesOnInsert добавляет документ во все индексы
func (c *Collection) updateIndexesOnInsert(docID string, doc map[string]any) {
	for fieldName, btree := range c.Indexes {
		if fieldValue, exists := doc[fieldName]; exists {
			key := index.ValueToKey(fieldValue)
			btree.Insert(key, []byte(docID))
		}
	}
}

// getByID получает документ по _id
func (c *Collection) GetByID(id string) (map[string]any, bool) {
	val, ok := c.Data.Get(id)
	if !ok {
		return nil, false
	}

	doc, ok := val.(map[string]any)
	if !ok {
		return nil, false
	}
	return doc, true
}

// delete удаляет документ по _id
func (c *Collection) Delete(id string) bool {
	// получаем документ перед удалением для обновления индексов
	doc, ok := c.GetByID(id)
	if !ok {
		return false
	}

	// удаляем из индексов
	c.updateIndexesOnDelete(id, doc)

	return c.Data.Remove(id)
}

func (c *Collection) updateIndexesOnDelete(docID string, doc map[string]any) {

}

func (c *Collection) All() []map[string]any {
	items := c.Data.Items()
	docs := make([]map[string]any, 0, len(items))
	for _, v := range items {
		if doc, ok := v.(map[string]any); ok {
			docs = append(docs, doc)
		}
	}
	return docs
}

func (c *Collection) CreateIndex(fieldName string, order int) error {

	if _, exists := c.Indexes[fieldName]; exists {
		return fmt.Errorf("index on field '%s' already exists", fieldName)
	}

	btree := index.NewBPlusTree(order)

	for _, doc := range c.All() {
		if fieldValue, exists := doc[fieldName]; exists {
			docID := doc["_id"].(string)
			key := index.ValueToKey(fieldValue)
			btree.Insert(key, []byte(docID))
		}
	}

	// сохраняем индекс
	c.Indexes[fieldName] = btree

	return c.SaveIndex(fieldName)
}

func (c *Collection) SaveIndex(fieldName string) error {
	btree, exists := c.Indexes[fieldName]
	if !exists {
		return fmt.Errorf("index on field '%s' does not exist", fieldName)
	}

	indexPath := filepath.Join("data", "indexes", fmt.Sprintf("%s_%s.idx", c.Name, fieldName))
	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	// сериализуем индекс
	indexData := serializeBTree(btree, fieldName, 64)
	jsonData, err := json.MarshalIndent(indexData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	if err := os.WriteFile(indexPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}

// loadIndex загружает индекс с диска
func (c *Collection) LoadIndex(fieldName string) error {
	indexPath := filepath.Join("data", "indexes", fmt.Sprintf("%s_%s.idx", c.Name, fieldName))

	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return nil // индекс не существует - не ошибка
	}

	jsonData, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	var indexData IndexFile
	if err := json.Unmarshal(jsonData, &indexData); err != nil {
		return fmt.Errorf("failed to unmarshal index: %w", err)
	}

	// десериализуем b-tree
	btree := deserializeBTree(&indexData)
	c.Indexes[fieldName] = btree

	return nil
}

// loadAllIndexes загружает все индексы для коллекции
func (c *Collection) LoadAllIndexes() error {
	indexDir := filepath.Join("data", "indexes")

	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return nil // нет директорий с индексами - не ошибка
	}

	entries, err := os.ReadDir(indexDir)
	if err != nil {
		return fmt.Errorf("failed to read index directory: %w", err)
	}

	// ищем индексы для этой коллекции
	prefix := c.Name + "_"
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if len(name) > len(prefix) && name[:len(prefix)] == prefix && filepath.Ext(name) == ".idx" {
			// извлекаем имя поля
			fieldName := name[len(prefix) : len(name)-4]
			if err := c.LoadIndex(fieldName); err != nil {
				return err
			}
		}
	}

	return nil
}

// hasIndex проверяет существование индекса на поле
func (c *Collection) HasIndex(fieldName string) bool {
	_, exists := c.Indexes[fieldName]
	return exists
}

// getIndex возвращает индекс для поля
func (c *Collection) GetIndex(fieldName string) (*index.BTree, bool) {
	btree, exists := c.Indexes[fieldName]
	return btree, exists
}

// saveAllIndexes сохраняет все индексы на диск
func (c *Collection) SaveAllIndexes() error {
	for fieldName := range c.Indexes {
		if err := c.SaveIndex(fieldName); err != nil {
			return err
		}
	}
	return nil
}

// rebuildAllIndexes пересоздает все индексы (для случая удаления)
func (c *Collection) RebuildAllIndexes() error {
	// сохраняем список полей для которых есть индексы
	fields := make([]string, 0, len(c.Indexes))
	for fieldName := range c.Indexes {
		fields = append(fields, fieldName)
	}

	// очищаем текущие индексы
	c.Indexes = make(map[string]*index.BTree)

	// пересоздаем индексы для каждого поля
	for _, fieldName := range fields {
		btree := index.NewBPlusTree(64)

		// сканируем все документы
		for _, doc := range c.All() {
			if fieldValue, exists := doc[fieldName]; exists {
				docID := doc["_id"].(string)
				key := index.ValueToKey(fieldValue)
				btree.Insert(key, []byte(docID))
			}
		}

		c.Indexes[fieldName] = btree

		// сохраняем на диск
		if err := c.SaveIndex(fieldName); err != nil {
			return err
		}
	}

	return nil
}

// indexFile структура для сохранения индекса
type IndexFile struct {
	Field string           `json:"field"`
	Order int              `json:"order"`
	Nodes []SerializedNode `json:"nodes"`
}

// serializedNode представляет сериализованный узел b-tree
type SerializedNode struct {
	IsLeaf   bool       `json:"is_leaf"`
	Keys     [][]byte   `json:"keys"`
	Values   [][][]byte `json:"values,omitempty"`
	Children []int      `json:"children,omitempty"`
}

// serializeBTree сериализует b-tree в структуру для json
func serializeBTree(tree *index.BTree, fieldName string, order int) *IndexFile {
	if tree == nil || tree.GetRoot() == nil {
		return &IndexFile{
			Field: fieldName,
			Order: order,
			Nodes: []SerializedNode{},
		}
	}

	var nodes []SerializedNode
	nodeIndexMap := make(map[*index.Node]int)

	// обход в ширину для нумерации узлов
	root := tree.GetRoot()
	queue := []*index.Node{root}
	idx := 0

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		nodeIndexMap[node] = idx
		idx++

		// добавляем дочерние узлы в очередь
		if !node.GetIsLeaf() {
			children := node.GetChildren()
			for _, child := range children {
				if child != nil {
					queue = append(queue, child)
				}
			}
		}
	}

	// сериализуем узлы
	queue = []*index.Node{root}
	visited := make(map[*index.Node]bool)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if visited[node] {
			continue
		}
		visited[node] = true

		serialized := SerializedNode{
			IsLeaf: node.GetIsLeaf(),
			Keys:   make([][]byte, 0),
		}

		// копируем ключи
		keys := node.GetKeys()
		for _, key := range keys {
			serialized.Keys = append(serialized.Keys, []byte(key))
		}

		if node.GetIsLeaf() {
			// для листьев копируем значения
			values := node.GetValues()
			serialized.Values = make([][][]byte, len(values))
			for i, vals := range values {
				serialized.Values[i] = make([][]byte, len(vals))
				for j, val := range vals {
					serialized.Values[i][j] = []byte(val)
				}
			}
		} else {
			// для внутренних узлов копируем индексы детей
			children := node.GetChildren()
			serialized.Children = make([]int, 0)
			for _, child := range children {
				if child != nil {
					serialized.Children = append(serialized.Children, nodeIndexMap[child])
					queue = append(queue, child)
				}
			}
		}

		nodes = append(nodes, serialized)
	}

	return &IndexFile{
		Field: fieldName,
		Order: order,
		Nodes: nodes,
	}
}

// deserializeBTree восстанавливает b-tree из сериализованных данных
func deserializeBTree(data *IndexFile) *index.BTree {
	if len(data.Nodes) == 0 {
		return index.NewBPlusTree(data.Order)
	}

	nodes := make([]*index.Node, len(data.Nodes))

	// создаем все узлы
	for i, sn := range data.Nodes {
		node := index.NewNode(sn.IsLeaf)

		// восстанавливаем ключи
		for _, key := range sn.Keys {
			node.AddKey(index.Key(key))
		}

		// восстанавливаем значения для листьев
		if sn.IsLeaf && len(sn.Values) > 0 {
			for _, vals := range sn.Values {
				var values []index.Value
				for _, val := range vals {
					values = append(values, index.Value(val))
				}
				node.AddValues(values)
			}
		}

		nodes[i] = node
	}

	// восстанавливаем связи между узлами
	for i, sn := range data.Nodes {
		if !sn.IsLeaf && len(sn.Children) > 0 {
			for _, childIdx := range sn.Children {
				if childIdx < len(nodes) {
					nodes[i].AddChild(nodes[childIdx])
					nodes[childIdx].SetParent(nodes[i])
				}
			}
		}
	}

	// восстанавливаем связь next для листьев
	var prevLeaf *index.Node
	for _, node := range nodes {
		if node.GetIsLeaf() {
			if prevLeaf != nil {
				prevLeaf.SetNext(node)
			}
			prevLeaf = node
		}
	}

	// создаем дерево с восстановленным корнем
	tree := index.NewBPlusTree(data.Order)
	tree.SetRoot(nodes[0])

	return tree
}
