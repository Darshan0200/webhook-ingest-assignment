package stats_test

import (
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

func TestCacheRecordConcurrent(t *testing.T) {
	c := stats.NewCache()
	acc := "acc_concurrent"
	
	const numGoroutines = 1000
	done := make(chan bool)
	
	for i := 0; i < numGoroutines; i++ {
		go func() {
			c.Record(acc, 1)
			done <- true
		}()
	}
	
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	got := c.Get(acc)
	if got.CallCount != numGoroutines {
		t.Fatalf("expected CallCount to be %d, got %d", numGoroutines, got.CallCount)
	}
}
