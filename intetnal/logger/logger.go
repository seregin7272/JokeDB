package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//nolint:gochecknoglobals //todo
var globalSugaredLogger *zap.SugaredLogger

func Init(devMode bool, name, level string) error {
	var config zap.Config
	levelEncoder := zapcore.CapitalLevelEncoder
	switch devMode {
	case true:
		config = zap.NewDevelopmentConfig()
		levelEncoder = zapcore.CapitalColorLevelEncoder
	default:
		config = zap.NewProductionConfig()
	}

	config.EncoderConfig.EncodeLevel = levelEncoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	err := config.Level.UnmarshalText([]byte(level))
	if err != nil || len(level) == 0 {
		config.Level.SetLevel(zap.DebugLevel)
	}

	config.Sampling = nil
	logger, err := config.Build()
	if err != nil {
		return err
	}
	logger.Info("Start service",
		zap.String("name", name),
		zap.String("service_loglevel", level))
	globalSugaredLogger = logger.Sugar()
	return nil
}

func L() *zap.SugaredLogger {
	return globalSugaredLogger
}
