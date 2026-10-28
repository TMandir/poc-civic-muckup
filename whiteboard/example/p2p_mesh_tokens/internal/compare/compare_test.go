package compare

import (
	"testing"
	"time"

	"sunya-p2p-mesh-tokens/internal/database"
)

func TestCompareTokenValues(t *testing.T) {
	store, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	for _, record := range []database.Record{
		{FrontendTokenKey: "first", Token: "one", Information: "a", UpdatedAt: time.Now(), Origin: "peer-a"},
		{FrontendTokenKey: "second", Token: "🧭two", Information: "b", UpdatedAt: time.Now(), Origin: "peer-b"},
	} {
		if _, err := store.ApplyIfNewer(record); err != nil {
			t.Fatalf("save record: %v", err)
		}
	}
	result, err := CompareTokenValues(store, "first", "second")
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result.FirstValueLength != 3 || result.SecondValueLength != 4 {
		t.Fatalf("lengths = %d and %d, want 3 and 4", result.FirstValueLength, result.SecondValueLength)
	}
	if got := ConcatenateTokenValues(result); got != "one🧭two" {
		t.Fatalf("concatenated value = %q", got)
	}
}
