package operators

import (
	"fmt"
	"reflect"
)

// CompareEq возвращает true, если fieldValue == queryValue
func CompareEq(fieldValue, queryValue any) bool {
	return reflect.DeepEqual(fieldValue, queryValue)
}

// CompareGt возвращает true, если fieldValue > queryValue
func CompareGt(fieldValue, queryValue any) bool {
	return compareNumeric(fieldValue, queryValue, func(a, b float64) bool {
		return a > b
	})
}

// CompareLt возвращает true, если fieldValue < queryValue
func CompareLt(fieldValue, queryValue any) bool {
	return compareNumeric(fieldValue, queryValue, func(a, b float64) bool {
		return a < b
	})
}

// CompareLike возвращает true, если fieldValue соответствует шаблону like
func CompareLike(fieldValue, pattern any) bool {
	fieldStr, ok1 := fieldValue.(string)
	patternStr, ok2 := pattern.(string)
	if !ok1 || !ok2 {
		return false
	}

	return matchLikePattern(fieldStr, patternStr)
}

// CompareIn возвращает true, если fieldValue содержится в values
func CompareIn(fieldValue any, values any) bool {
	valuesSlice, ok := values.([]any)
	if !ok {
		return false
	}

	for _, v := range valuesSlice {
		if reflect.DeepEqual(fieldValue, v) {
			return true
		}
	}
	return false
}

// compareNumeric вспомогательная функция для сравнения числовых значений
func compareNumeric(a, b any, cmp func(float64, float64) bool) bool {
	aNum, err1 := toFloat64(a)
	bNum, err2 := toFloat64(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return cmp(aNum, bNum)
}

// toFloat64 вспомогательная функция для конвертации в float64
func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

// matchLikePattern вспомогательная функция для сопоставления строки с шаблоном like
func matchLikePattern(str, pattern string) bool {
	return matchLikeHelper(str, pattern, 0, 0)
}

// matchLikeHelper функция для сопоставления строки с like
func matchLikeHelper(str, pattern string, strIdx, patIdx int) bool {

	// strIdx и patIdx - текущие индексы в строке и паттерне
	if strIdx == len(str) && patIdx == len(pattern) {
		return true
	}

	if patIdx == len(pattern) {
		return false
	}

	// обработка %
	if pattern[patIdx] == '%' {
		for patIdx < len(pattern) && pattern[patIdx] == '%' {
			patIdx++
		}

		if patIdx == len(pattern) {
			return true
		}

		for i := strIdx; i <= len(str); i++ {
			if matchLikeHelper(str, pattern, i, patIdx) {
				return true
			}
		}
		return false
	}

	if strIdx == len(str) {
		return false
	}

	// обработка _
	if pattern[patIdx] == '_' {
		return matchLikeHelper(str, pattern, strIdx+1, patIdx+1)
	}

	// обработка обычных символов
	if str[strIdx] == pattern[patIdx] {
		return matchLikeHelper(str, pattern, strIdx+1, patIdx+1)
	}

	return false
}
