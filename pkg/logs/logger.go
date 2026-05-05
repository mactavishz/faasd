package logs

import (
	"os"
	"strings"

	"go.uber.org/zap"
)

func GetEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func CreateLogger() *zap.Logger {
	var logger *zap.Logger
	if strings.ToLower(GetEnvOrDefault("ENV", "development")) == "development" {
		logger = zap.Must(zap.NewDevelopment())
	} else {
		logger = zap.Must(zap.NewProduction())
	}
	return logger
}
