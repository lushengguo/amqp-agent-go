package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server struct {
		Port int    `yaml:"port"`
		Host string `yaml:"host"`
	} `yaml:"server"`

	Queue struct {
		MaxSize string `yaml:"max_size"`
	} `yaml:"queue"`

	Log struct {
		Level        string `yaml:"level"`
		FilePath     string `yaml:"file_path"`
		MaxAge       int    `yaml:"max_age"`
		RotationTime int    `yaml:"rotation_time"`
	} `yaml:"log"`
}

func LoadConfig() (*Config, error) {
	data, err := os.ReadFile("config/amqp-agent.yaml")
	if err != nil {
		data, err = os.ReadFile("../config/amqp-agent.yaml")
		if err != nil {
			return nil, fmt.Errorf("error reading configuration file: %v", err)
		}
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing configuration file: %v", err)
	}

	if config.Log.Level == "" {
		config.Log.Level = "info"
	}
	if config.Log.FilePath == "" {
		config.Log.FilePath = "logs/amqp-agent.log"
	}
	if config.Log.MaxAge == 0 {
		config.Log.MaxAge = 7
	}
	if config.Log.RotationTime == 0 {
		config.Log.RotationTime = 24
	}

	return &config, nil
}

func parseSize(sizeStr string) (uint64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	var multiplier uint64 = 1
	var size uint64
	var unit string

	_, err := fmt.Sscanf(sizeStr, "%d%s", &size, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %v", err)
	}

	switch strings.ToUpper(unit) {
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported unit: %s", unit)
	}

	return size * multiplier, nil
}

func (c *Config) GetMaxSize() uint64 {
	size, err := parseSize(c.Queue.MaxSize)
	if err != nil {
		panic(fmt.Sprintf("error parsing max size: %v", err))
	}
	return size
}
