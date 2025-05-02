package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	// "runtime"
	"strings"
	"sync"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

var (
	configInstance     *Config
	loggerInstance     *logrus.Logger
	retryQueueInstance *RetryQueue
	statisticInstance  *Statistic
	onceLogger         sync.Once
	onceQueue          sync.Once
	onceStat           sync.Once
)

type CustomFormatter struct {
	TimestampFormat string
}

func (f *CustomFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format(f.TimestampFormat)
	var fileInfo string
	if entry.HasCaller() {
		fileInfo = fmt.Sprintf("%s:%d", filepath.Base(entry.Caller.File), entry.Caller.Line)
	}

	msg := fmt.Sprintf("%s %s %s %s\n",
		timestamp,
		strings.ToUpper(entry.Level.String()),
		entry.Message,
		fileInfo,
	)

	return []byte(msg), nil
}

func GetLogger() *logrus.Logger {
	onceLogger.Do(func() {
		if configInstance == nil {
			panic("configInstance is nil, please load the config first")
		}

		loggerInstance = logrus.New()
		level, err := logrus.ParseLevel(configInstance.Log.Level)
		if err != nil {
			level = logrus.InfoLevel
		}
		loggerInstance.SetLevel(level)

		loggerInstance.SetFormatter(&CustomFormatter{
			TimestampFormat: "2006-01-02 15:04:05",
		})
		loggerInstance.SetReportCaller(true)

		logDir := filepath.Dir(configInstance.Log.FilePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			panic(fmt.Sprintf("failed to create log directory: %v", err))
		}

		maxAge := 7 * 24 * time.Hour
		rotationTime := 24 * time.Hour

		if configInstance.Log.MaxAge > 0 {
			maxAge = time.Duration(configInstance.Log.MaxAge) * 24 * time.Hour
		}

		if configInstance.Log.RotationTime > 0 {
			rotationTime = time.Duration(configInstance.Log.RotationTime) * time.Hour
		}

		writer, err := rotatelogs.New(
			configInstance.Log.FilePath+".%Y%m%d",
			rotatelogs.WithMaxAge(maxAge),
			rotatelogs.WithRotationTime(rotationTime),
		)
		if err != nil {
			panic(fmt.Sprintf("failed to create log file writer: %v", err))
		}

		mw := io.MultiWriter(os.Stdout, writer)
		loggerInstance.SetOutput(mw)
	})
	return loggerInstance
}

func GetRetryQueue() *RetryQueue {
	onceQueue.Do(func() {
		if configInstance == nil {
			panic("configInstance is nil, please load the config first")
		}

		retryQueueInstance = NewRetryQueue(configInstance.GetMaxSize())
	})
	return retryQueueInstance
}

func GetStatistic() *Statistic {
	onceStat.Do(func() {
		statisticInstance = &Statistic{statistic: make(map[string]*eachLocationStatistic)}
	})
	return statisticInstance
}

func PeriodicallyFlushLogLevel() {
	tickS := 1
	ticker := time.NewTicker(time.Duration(tickS) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		config, err := LoadConfig()
		if err != nil {
			GetLogger().Warnf("Error loading configuration: %v\n", err)
			continue	
		}

		level, err := logrus.ParseLevel(config.Log.Level)
		if err != nil {
			level = logrus.InfoLevel
		}
		GetLogger().SetLevel(level)
	}
}

func main() {
	config, err := LoadConfig()
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}
	configInstance = config

	GetLogger().Info("Starting AMQP Agent")

	go PeriodicallyStatisticReport()
	go PeriodicallyReproduceFailedMessage()
	go PeriodicallyFlushLogLevel()

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		GetLogger().Errorf("Error starting server: %v", err)
	}
	defer listener.Close()

	GetLogger().Infof("Server started at %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			GetLogger().Errorf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}
