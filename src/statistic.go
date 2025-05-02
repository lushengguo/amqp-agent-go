package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type eachLocationStatistic struct {
	producedCount  uint64
	confirmedCount uint64
}

type Statistic struct {
	statistic map[string]*eachLocationStatistic
	mu        sync.Mutex
}

func (s *Statistic) IncrementProduced(locator string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.statistic[locator]; !ok {
		s.statistic[locator] = &eachLocationStatistic{}
		s.statistic[locator].producedCount = 1
	} else {
		s.statistic[locator].producedCount++
	}
}

func (s *Statistic) IncrementConfirmed(locator string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.statistic[locator].confirmedCount++
}

func (s *Statistic) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stat := range s.statistic {
		stat.producedCount = 0
		stat.confirmedCount = 0
	}
}

func (s *Statistic) statisticResult() map[string]eachLocationStatistic {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]eachLocationStatistic)
	for k, v := range s.statistic {
		result[k] = *v
	}
	return result
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
	tickS := 20
	ticker := time.NewTicker(time.Duration(tickS) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m := GetStatistic().statisticResult()
		GetStatistic().Reset()
		for k, v := range m {
			GetStatistic().Reset()
			GetLogger().Infof("Statistics - Within %d seconds: %s Received:%d, confirmed:%d",
				tickS, k, v.producedCount, v.confirmedCount)
		}
		GetLogger().Infof("Statistics - Memory usage: %s", getMemoryStatistic())
	}
}
