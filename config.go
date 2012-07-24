package aostor

import (
	"code.google.com/p/goconf/conf"
)

var DefaultConfigFile = "aostor.ini"

func ReadConf(fn string) (*conf.ConfigFile, error) {
	if fn == "" {
		fn = DefaultConfigFile
	}
	return conf.ReadConfigFile(fn)
}
