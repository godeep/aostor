package aostor

import (
	//"code.google.com/p/goconf/conf"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"github.com/kless/goconfig/config"
	"hash"
	"os"
	"strings"
)

const (
	DefaultConfigFile     = "aostor.ini"
	DefaultTarThreshold   = 1000 * (1 << 20) // 1000Mb
	DefaultIndexThreshold = 10               // How many index cdb should be merged
	DefaultContentHash    = "sha1"
	DefaultHostport       = ":8341"
	TestConfig            = `[dirs]
base = /tmp/aostor
staging = %(base)s/#(realm)s/staging
index = %(base)s/#(realm)s/ndx
tar = %(base)s/#(realm)s/store

[threshold]
index = 2
tar = 512

[http]
hostport = :8431
realms = test

[hash]
content = sha1
`
)

var (
	ConfigFile = DefaultConfigFile
	configs    = make(map[string]Config, 2) // configs cache
)

// configuration variables, parsed
type Config struct {
	StagingDir, IndexDir, TarDir string
	IndexThreshold               uint
	TarThreshold                 uint64
	Hostport                     string
	Realms                       []string
	ContentHash                  string
	ContentHashFunc              func() hash.Hash
}

// reads config file (or ConfigFile if empty), replaces every #(realm)s with the
// given realm, if given
func ReadConf(fn string, realm string) (Config, error) {
	k := fn + "#" + realm
	c, ok := configs[k]
	if ok {
		return c, nil
	}
	c, err := readConf(fn, realm)
	if err != nil {
		return Config{}, err
	}
	configs[k] = c
	return c, nil
}

func readConf(fn string, realm string) (Config, error) {
	var c Config
	if fn == "" {
		fn = ConfigFile
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
	for i := 0; i < 2; i++ {
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
	} else {
		c.IndexThreshold = uint(i)
	}

	i, err = conf.Int("threshold", "tar")
	if err != nil {
		logger.Printf("cannot get threshold/tar: %s", err)
		c.TarThreshold = DefaultTarThreshold
	} else {
		c.TarThreshold = uint64(i)
	}

	hp, err := conf.String("http", "hostport")
	if err != nil {
		logger.Printf("cannot get hostport: %s", err)
		c.Hostport = DefaultHostport
	} else {
		c.Hostport = hp
	}

	realms, err := conf.String("http", "realms")
	if err != nil {
		logger.Printf("cannot get realms: %s", err)
	} else {
		c.Realms = strings.Split(realms, ",")
	}

	hash, err := conf.String("hash", "content")
	if err != nil {
		logger.Printf("cannot get content hash: %s", err)
		hash = DefaultContentHash
		err = nil
	}
	c.ContentHash = hash
	switch hash {
	case "sha512":
		c.ContentHashFunc = sha512.New
	case "sha256":
		c.ContentHashFunc = sha256.New
	default:
		c.ContentHashFunc = sha1.New
		c.ContentHash = "sha1"
	}

	return c, err
}

func getDir(conf *config.Config, section string, option string, realm string) (string, error) {
	path, err := conf.String(section, option)
	if err != nil {
		return "", err
	}
	if realm != "" {
		path = strings.Replace(path, "#(realm)s", realm, -1)
		if !fileExists(path) {
			os.MkdirAll(path, 0755)
		}
	}
	return path, nil
}
