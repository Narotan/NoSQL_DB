package storage

const initialCapacity = 16
const loadFactor = 0.75

type Pair struct {
	Key   string
	Value any
	Next  *Pair
}

type HashMap struct {
	Buckets  []*Pair
	Size     int
	Capacity int
}

func NewHashMap() *HashMap {
	return &HashMap{
		Buckets:  make([]*Pair, initialCapacity),
		Capacity: initialCapacity,
		Size:     0,
	}
}

// hash (полиномиальная хеш-функция)
func (h *HashMap) Hash(key string) int {
	var hash uint32
	const prime uint32 = 3

	for i := 0; i < len(key); i++ {
		hash = hash*prime + uint32(key[i])
	}

	if h.Capacity == 0 {
		return 0
	}
	return int(hash) % h.Capacity
}

func (h *HashMap) Put(key string, value any) {
	if float64(h.Size)/float64(h.Capacity) >= loadFactor {
		h.resize()
	}

	index := h.Hash(key)
	current := h.Buckets[index]

	for current != nil {
		if current.Key == key {
			current.Value = value
			return
		}
		current = current.Next
	}

	newPair := &Pair{
		Key:   key,
		Value: value,
		Next:  h.Buckets[index],
	}

	h.Buckets[index] = newPair
	h.Size++
}

func (h *HashMap) Get(key string) (any, bool) {
	index := h.Hash(key)
	current := h.Buckets[index]

	for current != nil {
		if current.Key == key {
			return current.Value, true
		}
		current = current.Next
	}

	return nil, false
}

func (h *HashMap) Remove(key string) bool {
	index := h.Hash(key)
	current := h.Buckets[index]
	var prev *Pair

	for current != nil {
		if current.Key == key {
			if prev == nil {
				h.Buckets[index] = current.Next
			} else {
				prev.Next = current.Next
			}
			h.Size--
			return true
		}
		prev = current
		current = current.Next
	}

	return false
}

func (h *HashMap) resize() {

	oldBuckets := h.Buckets

	newCapacity := h.Capacity * 2
	h.Buckets = make([]*Pair, newCapacity)
	h.Capacity = newCapacity

	for _, head := range oldBuckets {
		current := head
		for current != nil {
			next := current.Next
			newIndex := h.Hash(current.Key)
			current.Next = h.Buckets[newIndex]
			h.Buckets[newIndex] = current

			current = next
		}
	}
}

func (h *HashMap) Items() map[string]any {
	allItems := make(map[string]any)
	for _, head := range h.Buckets {
		current := head
		for current != nil {
			allItems[current.Key] = current.Value
			current = current.Next
		}
	}

	return allItems
}
