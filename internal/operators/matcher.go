package operators

import (
	"fmt"
)

// MatchDocument проверяет, соответствует ли документ условиям запроса
func MatchDocument(doc map[string]any, query map[string]any) bool {
	if len(query) == 0 {
		return true
	}

	if orConditions, ok := query["$or"]; ok {
		return matchOr(doc, orConditions)
	}

	if andConditions, ok := query["$and"]; ok {
		return matchAnd(doc, andConditions)
	}

	// неявный AND - все условия должны выполняться
	for field, condition := range query {
		if !matchField(doc, field, condition) {
			return false
		}
	}

	return true
}

// matchField проверяет соответствие одного поля условию
func matchField(doc map[string]any, field string, condition any) bool {
	fieldValue, exists := doc[field]

	if !exists {
		return false
	}

	// если condition - это map, значит это операторы сравнения
	if condMap, ok := condition.(map[string]any); ok {
		for operator, value := range condMap {
			if !applyOperator(fieldValue, operator, value) {
				return false
			}
		}
		return true
	}

	return CompareEq(fieldValue, condition)
}

// applyOperator применяет оператор к значению поля
func applyOperator(fieldValue any, operator string, queryValue any) bool {
	switch operator {
	case "$eq":
		return CompareEq(fieldValue, queryValue)
	case "$gt":
		return CompareGt(fieldValue, queryValue)
	case "$lt":
		return CompareLt(fieldValue, queryValue)
	case "$like":
		return CompareLike(fieldValue, queryValue)
	case "$in":
		return CompareIn(fieldValue, queryValue)
	default:
		fmt.Printf("Warning: unknown operator %s\n", operator)
		return false
	}
}

// matchOr проверяет логический оператор $or
func matchOr(doc map[string]any, orConditions any) bool {
	conditions, ok := orConditions.([]any)
	if !ok {
		return false
	}

	for _, cond := range conditions {
		condMap, ok := cond.(map[string]any)
		if !ok {
			continue
		}
		if MatchDocument(doc, condMap) {
			return true
		}
	}

	return false
}

// matchAnd проверяет логический оператор $and
func matchAnd(doc map[string]any, andConditions any) bool {
	conditions, ok := andConditions.([]any)
	if !ok {
		return false
	}

	for _, cond := range conditions {
		condMap, ok := cond.(map[string]any)
		if !ok {
			return false
		}
		if !MatchDocument(doc, condMap) {
			return false
		}
	}

	return true
}
