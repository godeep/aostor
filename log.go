// Copyright 2012 cihub
// Copied verbatim from https://github.com/cihub/seelog/wiki/Writing-libraries-with-Seelog

//
// Log is disabled by default
// Specific logger can be passed to the library using a 'aostor.UseLogger(...)' call
// You can enable library log without importing Seelog with a 'aostor.SetLogWriter(writer)' call
//
package aostor

import (
	"errors"
	seelog "github.com/cihub/seelog"
	"io"
	"log"
)

var logger seelog.LoggerInterface
var DefaultLogConf = `<seelog minlevel="debug">
    <outputs>
        <console formatid="console" />
    </outputs>
    <formats>
    	<format id="console" format="[%LEV] %Msg%n" />
    	<format id="file" format="%Date %Time [%LEV] %RelFile %Func: %Msg%n" />
    </formats>
</seelog>`

func init() {
	DisableLog()
}

// DisableLog disables all library log output
func DisableLog() {
	logger = seelog.Disabled
}

func LogIsDisabled() bool {
	return logger == seelog.Disabled
}

func GetLogger() seelog.LoggerInterface {
	return logger
}

// UseLogger uses a specified seelog.LoggerInterface to output library log.
// Use this func if you are using Seelog logging system in your app.
func UseLogger(newLogger seelog.LoggerInterface) {
	if newLogger != nil {
		logger = newLogger
	}
}

// loads logger from config file
func UseLoggerFromConfigFile(filename string) {
	if filename == "" {
		filename = "seelog.xml"
	}
	newLogger, err := seelog.LoggerFromConfigAsFile(filename)
	if err != nil {
		log.Printf("cannot read %s: %s", filename, err)
		if LogIsDisabled() {
			newLogger, e := seelog.LoggerFromConfigAsString(DefaultLogConf)
			if e != nil {
				log.Printf("cannot load logger config: %s", e)
			}
			UseLogger(newLogger)
		}
		logger.Error("cannot read %s: %s", filename, err)
	} else {
		UseLogger(newLogger)
	}
}

// SetLogWriter uses a specified io.Writer to output library log.
// Use this func if you are not using Seelog logging system in your app.
func SetLogWriter(writer io.Writer) error {
	if writer == nil {
		return errors.New("Nil writer")
	}

	newLogger, err := seelog.LoggerFromWriterWithMinLevel(writer, seelog.TraceLvl)
	if err != nil {
		return err
	}

	UseLogger(newLogger)
	return nil
}

// Call this before app shutdown
func FlushLog() {
	logger.Flush()
}
