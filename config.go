package aostor

import (
	//"code.google.com/p/goconf/conf"
	"fmt"
	"github.com/kless/goconfig/config"
	"os"
	"strings"
)

const (
	DefaultConfigFile     = "aostor.ini"
	DefaultTarThreshold   = 1000 * (1 << 20)
	DefaultIndexThreshold = 10
	TestConfig            = `[dirs]
base = /tmp/aostor
staging = %(base)s/#(realm)s/staging
index = %(base)s/#(realm)s/ndx
tar = %(base)s/#(realm)s/store

[threshold]
index = 2
tar = 512
`
)

var ConfigFile = DefaultConfigFile

type Config struct {
	StagingDir, IndexDir, TarDir string
	IndexThreshold               uint
	TarThreshold                 uint64
}

func ReadConf(fn string, realm string) (Config, error) {
	var c Config
	if fn == "" {
		fn = DefaultConfigFile
	}
	conf, err := config.ReadDefault(fn)
	if err != nil {
		return c, err
	}

	c.StagingDir, err = getDir(conf, "dirs", "staging", realm)
	if err != nil {
		return c, err
	}

	c.IndexDir, err = getDir(conf, "dirs", "index", realm)
	if err != nil {
		return c, err
	}
	for i := 0; i < 10; i++ {
		dn := c.IndexDir + "/" + fmt.Sprintf("L%02d", i)
		if !fileExists(dn) {
			if err = os.MkdirAll(dn, 0755); err != nil {
				return c, err
			}
		}
	}

	c.TarDir, err = getDir(conf, "dirs", "tar", realm)
	if err != nil {
		return c, err
	}

	i, err := conf.Int("threshold", "index")
	if err != nil {
		logger.Printf("cannot get threshold/index: %s", err)
		c.IndexThreshold = DefaultIndexThreshold
	}
	c.IndexThreshold = uint(i)

	i, err = conf.Int("threshold", "tar")
	if err != nil {
		logger.Printf("cannot get threshold/tar: %s", err)
		c.TarThreshold = DefaultTarThreshold
	}
	c.TarThreshold = uint64(i)
	return c, err
}

func getDir(conf *config.Config, section string, option string, realm string) (string, error) {
	path, err := conf.String(section, option)
	if err != nil {
		return "", err
	}
	if realm != "" {
		path = strings.Replace(path, "#(realm)s", realm, -1)
	}
	if !fileExists(path) {
		os.MkdirAll(path, 0755)
	}
	return path, nil
}
