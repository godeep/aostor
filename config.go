package aostor

import (
	"os"
	"code.google.com/p/goconf/conf"
)

const DefaultConfigFile = "aostor.ini"
var ConfigFile = DefaultConfigFile

func ReadConf(fn string) (*conf.ConfigFile, error) {
	if fn == "" {
		fn = DefaultConfigFile
	}
	config, err := conf.ReadConfigFile(fn)
	if err != nil {
		return nil, err
	}
	if staging_dir, err := config.GetString("dirs", "staging"); err == nil {
		if !fileExists(staging_dir) {
			os.MkdirAll(staging_dir, 0755)
		}
	}
	return config, err
}
