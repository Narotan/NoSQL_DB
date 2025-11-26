package index

import "bytes"

type Key []byte
type Value []byte

type Node struct {
	isLeaf   bool
	keys     []Key
	values   [][]Value
	children []*Node
	next     *Node
	parent   *Node
}

type BTree struct {
	root  *Node
	order int
}

// NewBPlusTree создаёт новый b+ tree с указанным order
func NewBPlusTree(order int) *BTree {
	return &BTree{
		root: &Node{
			isLeaf: true,
			keys:   []Key{},
			values: [][]Value{},
		},
		order: order,
	}
}

// Insert вставляет ключ и значение в дерево
func (tree *BTree) Insert(key Key, value Value) {

	if tree.root == nil {
		NewBPlusTree(tree.order).Insert(key, value)
	}

	// поиск листа в дереве для вставки ключа
	leaf := tree.findLeaf(tree.root, key)

	// вставляем ключ и значение в лист
	tree.insertInLeaf(leaf, key, value)

	// если лист переполнен, разделим его
	if len(leaf.keys) > tree.order*2-1 {
		tree.splitLeaf(leaf)
	}
}

// findLeaf возвращает лист для заданного ключа
func (tree *BTree) findLeaf(node *Node, key Key) *Node {
	if node.isLeaf {
		return node
	}

	// смотрим куда идем: вправо или влево по разделению
	for i, k := range node.keys {
		if bytes.Compare(key, k) < 0 {
			return tree.findLeaf(node.children[i], key)
		}
	}

	return tree.findLeaf(node.children[len(node.children)-1], key)
}

// insertInLeaf вставляет ключ и значение в лист
func (tree *BTree) insertInLeaf(leaf *Node, key Key, value Value) {
	pos := 0
	for pos < len(leaf.keys) && bytes.Compare(leaf.keys[pos], key) < 0 {
		pos++
	}

	// если ключ уже есть, добавляем значение в массив
	if pos < len(leaf.keys) && bytes.Equal(leaf.keys[pos], key) {
		leaf.values[pos] = append(leaf.values[pos], value)
		return
	}

	leaf.keys = append(leaf.keys, nil)
	copy(leaf.keys[pos+1:], leaf.keys[pos:])
	leaf.keys[pos] = key

	leaf.values = append(leaf.values, nil)
	copy(leaf.values[pos+1:], leaf.values[pos:])
	leaf.values[pos] = []Value{value}
}

// splitLeaf разделяет лист и добавляет новый лист в связный список
func (tree *BTree) splitLeaf(leaf *Node) {
	mid := len(leaf.keys) / 2

	// создаем новый лист где пойдет вторая половина ключей и значений
	newLeaf := &Node{
		isLeaf: true,
		keys:   append([]Key{}, leaf.keys[mid:]...),
		values: append([][]Value{}, leaf.values[mid:]...),
		next:   leaf.next,
		parent: leaf.parent,
	}

	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]
	leaf.next = newLeaf

	// после того как мы сплитнули лист, нужно поднимать ключ в родителя
	// потом он должен знать о новом листе
	tree.insertInParent(leaf, newLeaf.keys[0], newLeaf)
}

// insertInParent поднимает ключ в родителя после сплита
// надо чтобы родитель знал когда идти в правое, а когда в левое поддерево
func (tree *BTree) insertInParent(left *Node, key Key, right *Node) {
	if left.parent == nil {
		newRoot := &Node{
			isLeaf:   false,
			keys:     []Key{key},
			children: []*Node{left, right},
		}
		left.parent = newRoot
		right.parent = newRoot
		tree.root = newRoot
		return
	}

	parent := left.parent

	pos := 0
	for pos < len(parent.keys) && bytes.Compare(parent.keys[pos], key) < 0 {
		pos++
	}

	parent.keys = append(parent.keys, nil)
	copy(parent.keys[pos+1:], parent.keys[pos:])
	parent.keys[pos] = key

	parent.children = append(parent.children, nil)
	copy(parent.children[pos+2:], parent.children[pos+1:])
	parent.children[pos+1] = right
	right.parent = parent

	// если родитель переполнен, то сплитим родителя
	if len(parent.keys) > tree.order*2-1 {
		tree.splitInternal(parent)
	}
}

// splitInternal разделяет внутренний узел и поднимает ключ в родителя
func (tree *BTree) splitInternal(node *Node) {
	mid := len(node.keys) / 2
	keyToPushUp := node.keys[mid]

	newNode := &Node{
		isLeaf:   false,
		keys:     append([]Key{}, node.keys[mid+1:]...),
		children: append([]*Node{}, node.children[mid+1:]...),
		parent:   node.parent,
	}

	for _, child := range newNode.children {
		child.parent = newNode
	}

	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	// поднимаем ключ в родителя
	tree.insertInParent(node, keyToPushUp, newNode)
}

// Search выполняет точечный поиск по ключу ($eq)
func (tree *BTree) Search(key Key) []Value {
	if tree.root == nil {
		return nil
	}

	leaf := tree.findLeaf(tree.root, key)

	// ищем ключ в листе
	for i, k := range leaf.keys {
		if bytes.Equal(k, key) {
			return leaf.values[i]
		}
	}

	return nil
}

// RangeSearch выполняет диапазонный поиск ($gt, $lt, $gte, $lte)
func (tree *BTree) RangeSearch(start, end Key, includeStart, includeEnd bool) []Value {
	if tree.root == nil {
		return nil
	}

	var result []Value

	var startLeaf *Node
	if start == nil {
		startLeaf = tree.findLeftmostLeaf(tree.root)
	} else {
		startLeaf = tree.findLeaf(tree.root, start)
	}

	// проходим по всем листьям через связанный список
	for leaf := startLeaf; leaf != nil; leaf = leaf.next {
		for i, k := range leaf.keys {
			if start != nil {
				cmp := bytes.Compare(k, start)
				if cmp < 0 || (cmp == 0 && !includeStart) {
					continue
				}
			}

			if end != nil {
				cmp := bytes.Compare(k, end)
				if cmp > 0 || (cmp == 0 && !includeEnd) {
					return result
				}
			}

			// добавляем все значения для этого ключа
			result = append(result, leaf.values[i]...)
		}

		// если достигли конца диапазона, выходим
		if end != nil && len(leaf.keys) > 0 {
			lastKey := leaf.keys[len(leaf.keys)-1]
			if bytes.Compare(lastKey, end) >= 0 {
				break
			}
		}
	}

	return result
}

// SearchGreaterThan ищет все значения где ключ > key ($gt)
func (tree *BTree) SearchGreaterThan(key Key) []Value {
	return tree.RangeSearch(key, nil, false, false)
}

// SearchLessThan ищет все значения где ключ < key ($lt)
func (tree *BTree) SearchLessThan(key Key) []Value {
	return tree.RangeSearch(nil, key, false, false)
}

// SearchGreaterThanOrEqual ищет все значения где ключ >= key ($gte)
func (tree *BTree) SearchGreaterThanOrEqual(key Key) []Value {
	return tree.RangeSearch(key, nil, true, false)
}

// SearchLessThanOrEqual ищет все значения где ключ <= key ($lte)
func (tree *BTree) SearchLessThanOrEqual(key Key) []Value {
	return tree.RangeSearch(nil, key, false, true)
}

// SearchIn выполняет множественный точечный поиск ($in)
// возвращает все значения для списка ключей
func (tree *BTree) SearchIn(keys []Key) []Value {
	var result []Value

	for _, key := range keys {
		values := tree.Search(key)
		if values != nil {
			result = append(result, values...)
		}
	}

	return result
}

// findLeftmostLeaf находит самый левый лист дерева
func (tree *BTree) findLeftmostLeaf(node *Node) *Node {
	if node == nil {
		return nil
	}

	for !node.isLeaf {
		if len(node.children) > 0 {
			node = node.children[0]
		} else {
			break
		}
	}

	return node
}

// GetAllValues возвращает все значения из дерева (для full scan)
func (tree *BTree) GetAllValues() []Value {
	if tree.root == nil {
		return nil
	}

	var result []Value
	leaf := tree.findLeftmostLeaf(tree.root)

	for leaf != nil {
		for _, values := range leaf.values {
			result = append(result, values...)
		}
		leaf = leaf.next
	}

	return result
}

// GetRoot возвращает корень дерева
func (tree *BTree) GetRoot() *Node {
	return tree.root
}

// SetRoot устанавливает корень дерева
func (tree *BTree) SetRoot(node *Node) {
	tree.root = node
}

// GetOrder возвращает порядок дерева
func (tree *BTree) GetOrder() int {
	return tree.order
}

// NewNode создаёт новый узел
func NewNode(isLeaf bool) *Node {
	return &Node{
		isLeaf:   isLeaf,
		keys:     []Key{},
		values:   [][]Value{},
		children: []*Node{},
	}
}

// GetIsLeaf возвращает флаг isLeaf
func (n *Node) GetIsLeaf() bool {
	return n.isLeaf
}

// GetKeys возвращает ключи узла
func (n *Node) GetKeys() []Key {
	return n.keys
}

// GetValues возвращает значения узла
func (n *Node) GetValues() [][]Value {
	return n.values
}

// GetChildren возвращает дочерние узлы
func (n *Node) GetChildren() []*Node {
	return n.children
}

// GetNext возвращает указатель на следующий лист
func (n *Node) GetNext() *Node {
	return n.next
}

// GetParent возвращает родителя узла
func (n *Node) GetParent() *Node {
	return n.parent
}

// AddKey добавляет ключ в узел
func (n *Node) AddKey(key Key) {
	n.keys = append(n.keys, key)
}

// AddValues добавляет значения в узел
func (n *Node) AddValues(values []Value) {
	n.values = append(n.values, values)
}

// AddChild добавляет дочерний узел
func (n *Node) AddChild(child *Node) {
	n.children = append(n.children, child)
}

// SetNext устанавливает next для узла
func (n *Node) SetNext(next *Node) {
	n.next = next
}

// SetParent устанавливает parent для узла
func (n *Node) SetParent(parent *Node) {
	n.parent = parent
}
