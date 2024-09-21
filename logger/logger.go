package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

var Logger *zap.Logger
var err error

func InitLogger() {
	ENV := os.Getenv("APP_ENV")
	var err error

	var config zap.Config

	switch ENV {
	case "DEV":
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig = zap.NewDevelopmentEncoderConfig()
	case "PROD":
		config = zap.NewProductionConfig()
		config.EncoderConfig = zap.NewProductionEncoderConfig()
	case "TEST":
		Logger = zap.NewExample()
		return
	default:
		Logger.Error("staging ENV not present, hint: check .env file")
	}

	Logger, err = config.Build()
	if err != nil {
		fmt.Printf("Error in initializing logger: %v\n", err)
		os.Exit(1)
	}

	Logger.Info("Log Initiated !!!")
}
