package destinations

import (
	"errors"
	"scratchdata/models"
	"scratchdata/pkg/destinations/memory"
	"testing"
)

func TestGetDestinations(t *testing.T) {
	cache := &destinationsCache{}
	db1, err := cache.Get(models.DatabaseConnection{ID: "id-a", Type: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	db2, err := cache.Get(models.DatabaseConnection{ID: "id-a", Type: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if db2 != db1 {
		t.Fatal("Cache failed to return the same instance on the second call")
	}

	db3, err := cache.Get(models.DatabaseConnection{ID: "id-b", Type: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if db3 == db2 {
		t.Fatal("Cache failed to return a new instance")
	}

	if err := cache.Clear(); err != nil {
		t.Fatal(err)
	}

	// MemoryDBServer returns ErrClosed if it's closed twice
	// so use that fact to detect whether cache.Clear() called Close()
	if err := db3.(*memory.MemoryDBServer).Close(); !errors.Is(err, memory.ErrClosed) {
		t.Fatalf("Cache failed to close cached instance: Expected %v; Got %v", memory.ErrClosed, err)
	}

	db4, err := cache.Get(models.DatabaseConnection{ID: "id-b", Type: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	if db4 == db3 {
		t.Fatal("Failed to clear cache; new db4 == old db3")
	}
}
