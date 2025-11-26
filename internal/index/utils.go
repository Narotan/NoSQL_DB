package index

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ValueToKey конвертирует значение в ключ для b-tree (массив байт)
func ValueToKey(value any) Key {
	switch v := value.(type) {
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case int32:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case float32:
		// json числа чаще float64
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(float64(v)))
		return buf
	case float64:
		// для сортировок float используем побитовое представление
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v))
		return buf
	case string:
		// строки просто конвертируем в []byte
		return []byte(v)
	case bool:
		if v {
			return []byte{1}
		}
		return []byte{0}
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// ValuesToStrings конвертирует массив value ([]byte) в массив строк (ids)
func ValuesToStrings(values []Value) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = string(v)
	}
	return result
}
