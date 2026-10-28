package main

import (
	"fmt"
	"unicode/utf8"
)

type tokenValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TokenComparison contains two locally stored values and their Unicode
// character counts.
type TokenComparison struct {
	First             tokenValue `json:"first"`
	FirstValueLength  int        `json:"firstValueLength"`
	Second            tokenValue `json:"second"`
	SecondValueLength int        `json:"secondValueLength"`
}

// CompareTokenValues reads both token records from this node's LevelDB store.
// Replication is handled by the peer network; this function never performs a
// remote lookup.
func CompareTokenValues(store *userTokenStore, firstKey, secondKey string) (TokenComparison, error) {
	first, err := store.Lookup(firstKey)
	if err != nil {
		return TokenComparison{}, fmt.Errorf("lookup first key %q: %w", firstKey, err)
	}
	second, err := store.Lookup(secondKey)
	if err != nil {
		return TokenComparison{}, fmt.Errorf("lookup second key %q: %w", secondKey, err)
	}
	return TokenComparison{
		First:             tokenValue{Key: first.FrontendTokenKey, Value: first.Token},
		FirstValueLength:  utf8.RuneCountInString(first.Token),
		Second:            tokenValue{Key: second.FrontendTokenKey, Value: second.Token},
		SecondValueLength: utf8.RuneCountInString(second.Token),
	}, nil
}

// ConcatenateTokenValues joins both values in their comparison order.
func ConcatenateTokenValues(comparison TokenComparison) string {
	return comparison.First.Value + comparison.Second.Value
}
