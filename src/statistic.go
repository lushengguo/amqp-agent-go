package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Statistic struct {
	producedCount     uint64
	confirmedCount      uint64
	lastReceivedCount uint64
	lastSuccessCount  uint64
	lastFailedCount   uint64
	mu                sync.Mutex
}

func (s *Statistic) IncrementProduced() {
	atomic.AddUint64(&s.producedCount, 1)
}

func (s *Statistic) IncrementConfirmed() {
	atomic.AddUint64(&s.confirmedCount, 1)
}

func (s *Statistic) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReceivedCount = s.producedCount
	s.lastSuccessCount = s.confirmedCount
}

func (s *Statistic) GetStats() (uint64, uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.producedCount - s.lastReceivedCount,
		s.confirmedCount - s.lastSuccessCount,
		s.producedCount - s.confirmedCount
}

func getMemoryStatistic() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("Alloc: %v MiB, Sys: %v MiB, NumGC: %v",
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC)
}

func PeriodicallyStatisticReport() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		received, success, failed := GetStatistic().GetStats()
		GetStatistic().Reset()
		GetLogger().Infof("Statistics - Within 20 seconds: Received messages: %d, Successfully sent: %d, Failed: %d, Memory usage: %s",
			received, success, failed, getMemoryStatistic())
	}
}