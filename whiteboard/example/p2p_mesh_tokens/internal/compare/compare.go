// Package compare compares records stored in a peer's local database.
package compare

import (
	"fmt"
	"unicode/utf8"

	"sunya-p2p-mesh-tokens/internal/database"
)

// Value contains a token key and its stored value.
type Value struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Result contains both values and their Unicode code-point lengths.
type Result struct {
	First             Value `json:"first"`
	FirstValueLength  int   `json:"firstValueLength"`
	Second            Value `json:"second"`
	SecondValueLength int   `json:"secondValueLength"`
}

// CompareTokenValues loads both records from this peer's local database.
// Network synchronization is performed by the peer package, not by this
// function, so comparison remains deterministic and works offline.
func CompareTokenValues(store *database.Store, firstKey, secondKey string) (Result, error) {
	first, err := store.Lookup(firstKey)
	if err != nil {
		return Result{}, fmt.Errorf("lookup first key %q: %w", firstKey, err)
	}
	second, err := store.Lookup(secondKey)
	if err != nil {
		return Result{}, fmt.Errorf("lookup second key %q: %w", secondKey, err)
	}
	return Result{
		First:             Value{Key: first.FrontendTokenKey, Value: first.Token},
		FirstValueLength:  utf8.RuneCountInString(first.Token),
		Second:            Value{Key: second.FrontendTokenKey, Value: second.Token},
		SecondValueLength: utf8.RuneCountInString(second.Token),
	}, nil
}

// ConcatenateTokenValues joins both values in comparison order.
func ConcatenateTokenValues(result Result) string {
	return result.First.Value + result.Second.Value
}
