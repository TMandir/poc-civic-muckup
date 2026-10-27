package main

import (
	"fmt"
	"unicode/utf8"
)

type tokenValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// tokenComparison holds each key and value alongside its separate value length.
type tokenComparison struct {
	First             tokenValue `json:"first"`
	FirstValueLength  int        `json:"firstValueLength"`
	Second            tokenValue `json:"second"`
	SecondValueLength int        `json:"secondValueLength"`
}

// CompareTokenValues loads two records by key and returns their values and
// Unicode character counts.
func CompareTokenValues(store *userTokenStore, firstKey, secondKey string) (tokenComparison, error) {
	first, err := store.Lookup(firstKey)
	if err != nil {
		return tokenComparison{}, fmt.Errorf("lookup first key %q: %w", firstKey, err)
	}

	second, err := store.Lookup(secondKey)
	if err != nil {
		return tokenComparison{}, fmt.Errorf("lookup second key %q: %w", secondKey, err)
	}

	return tokenComparison{
		First: tokenValue{
			Key:   first.FrontendTokenKey,
			Value: first.Token,
		},
		FirstValueLength: utf8.RuneCountInString(first.Token),
		Second: tokenValue{
			Key:   second.FrontendTokenKey,
			Value: second.Token,
		},
		SecondValueLength: utf8.RuneCountInString(second.Token),
	}, nil
}

// ConcatenateTokenValues joins two token values in their comparison order.
func ConcatenateTokenValues(comparison tokenComparison) string {
	return comparison.First.Value + comparison.Second.Value
}
