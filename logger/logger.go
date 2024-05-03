package logger

import (
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

var once sync.Once
var zLogger zerolog.Logger
var DebugMode bool

type LevelWriter zerolog.LevelWriter

type LevelWriterAdapter struct {
	zerolog.LevelWriterAdapter
	Level zerolog.Level
}

// GetLogger returns a logger instance, initializing it if necessary
func GetLogger() zerolog.Logger {
	// ensure that the logger is only created once
	once.Do(func() {
		zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

		// set default log level to ERROR
		logLevelInt, err := strconv.Atoi(os.Getenv("LOG_LEVEL"))
		var logLevel zerolog.Level
		logLevel = zerolog.Level(logLevelInt)
		if err != nil {
			logLevel = zerolog.InfoLevel // default to INFO
			// logLevel = int(zerolog.ErrorLevel) // default to ERROR
			// logLevel = int(zerolog.DebugLevel) // default to DEBUG
		}

		// set default output to stdout
		var output io.Writer = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}

		// TODO: this should obviously get set in the makefile/config/elsewhere,
		// just leaving it here for now so we can test the logging to file
		// os.Setenv("APP_ENV", "dev")
		// if the env is not development, log to file
		if !DebugMode {
			// TODO: get log directory from config
			// TODO: decide on log file naming convention
			// TODO: roll logs https://gist.github.com/panta/2530672ca641d953ae452ecb5ef79d7d
			file, err := os.OpenFile(
				"rita.log",
				os.O_APPEND|os.O_CREATE|os.O_WRONLY,
				0664,
			)

			if err != nil {
				panic(err)
			}

			var fileWriter LevelWriter = LevelWriterAdapter{Level: logLevel, LevelWriterAdapter: zerolog.LevelWriterAdapter{Writer: file}}

			fileLogger := &zerolog.FilteredLevelWriter{
				Writer: fileWriter,
				Level:  logLevel,
			}

			var stdWriter LevelWriter = LevelWriterAdapter{Level: zerolog.InfoLevel, LevelWriterAdapter: zerolog.LevelWriterAdapter{Writer: output}}

			stdLogger := &zerolog.FilteredLevelWriter{
				Writer: stdWriter,
				Level:  logLevel,
			}

			// this would log to both stderr and file
			output = zerolog.MultiLevelWriter(stdLogger, fileLogger)
			zLogger = zerolog.New(output).With().Timestamp().Logger()

			// log to file only
			// output = file
		} else {
			logLevelInt = int(zerolog.DebugLevel)
			zLogger = zerolog.New(output).Level(zerolog.Level(logLevel)).With().Timestamp().Logger()
		}

	})

	return zLogger
}

func (lw LevelWriterAdapter) WriteLevel(l zerolog.Level, p []byte) (n int, err error) {
	if l >= lw.Level {
		return lw.Write(p)
	}
	return 0, nil
}
