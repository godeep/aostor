// Copyright 2012 Tamás Gulácsi, UNO-SOFT Computing Ltd.
//
// All rights reserved.
//
// This file is part of aostor.
//
// Aostor is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Aostor is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Aostor.  If not, see <http://www.gnu.org/licenses/>.

package aostor

import (
	"bufio"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"github.com/kless/goconfig/config"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	DefaultConfigFile     = "aostor.ini"
	DefaultTarThreshold   = 1000 * (1 << 20) // 1000Mb
	DefaultIndexThreshold = 10               // How many index cdb should be merged
	DefaultContentHash    = "sha1"
	DefaultHostport       = ":8341"
	DefaultLogConf        = "seelog.xml"
	DefaultCommandPipe    = "command.pipe"
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

[log]
config = seelog.xml
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
	LogConf                      string
	Commands                     Controller
}

// reads config file (or ConfigFile if empty), replaces every #(realm)s with the
// given realm, if given
func ReadConf(fn string, realm string) (Config, error) {
	k_def := fn + "#"
	k := k_def + realm
	c, ok := configs[k]
	if ok {
		return c, nil
	}
	if LogIsDisabled() {
		UseLoggerFromConfigFile(DefaultLogConf)
	}
	c, err := readConf(fn, realm, configs[k_def])
	if err != nil {
		logger.Error("cannot open config: %s", err)
		return Config{}, err
	}
	if _, ok := configs[k_def]; !ok {
		configs[k_def] = c
	}
	configs[k] = c
	return c, nil
}

func readConf(fn string, realm string, common Config) (Config, error) {
	var c Config
	if fn == "" {
		fn = ConfigFile
	}
	conf, err := config.ReadDefault(fn)
	if err != nil {
		return c, err
	}
	if common.LogConf != "" {
		c.LogConf = common.LogConf
	} else {
		logconf, err := conf.String("log", "config")
		if err != nil {
			fmt.Printf("cannot get log configuration: %s", err)
			c.LogConf = DefaultLogConf
		} else {
			UseLoggerFromConfigFile(logconf)
			c.LogConf = logconf
		}
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

	if common.IndexThreshold > 0 {
		c.IndexThreshold = common.IndexThreshold
	} else {
		i, err := conf.Int("threshold", "index")
		if err != nil {
			logger.Warn("cannot get threshold/index: %s", err)
			c.IndexThreshold = DefaultIndexThreshold
		} else {
			c.IndexThreshold = uint(i)
		}
	}

	if common.TarThreshold > 0 {
		c.TarThreshold = common.TarThreshold
	} else {
		i, err := conf.Int("threshold", "tar")
		if err != nil {
			logger.Warn("cannot get threshold/tar: %s", err)
			c.TarThreshold = DefaultTarThreshold
		} else {
			c.TarThreshold = uint64(i)
		}
	}

	if common.Hostport != "" {
		c.Hostport = common.Hostport
	} else {
		hp, err := conf.String("http", "hostport")
		if err != nil {
			logger.Warn("cannot get hostport: %s", err)
			c.Hostport = DefaultHostport
		} else {
			c.Hostport = hp
		}
	}

	if len(common.Realms) > 0 {
		c.Realms = common.Realms
	} else {
		realms, err := conf.String("http", "realms")
		if err != nil {
			logger.Warn("cannot get realms: %s", err)
		} else {
			c.Realms = strings.Split(realms, ",")
		}
	}

	hash := DefaultContentHash
	if common.ContentHash != "" {
		hash = common.ContentHash
	} else {
		hash, err = conf.String("hash", "content")
		if err != nil {
			logger.Warn("cannot get content hash: %s", err)
			err = nil
		}
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

	if common.Commands != nil {
		c.Commands = common.Commands
	} else {
		pipefn, err := conf.String("http", "command_pipe")
		if err != nil {
			logger.Warn("cannot get command_pipe: %s", err)
			pipefn = filepath.Dir(c.StagingDir) + "/" + DefaultCommandPipe
			if realm != "" {
				pipefn = strings.Replace(pipefn, "/" + realm + "/", "/#(realm)s/", -1)
			}
		}
		if pipefn != "" {
			pipefn = strings.Replace(strings.Replace(pipefn, "#(realm)s", "", -1), "//", "/", -1) // no realm
			logger.Info("pipefn=%s", pipefn)
			if fifoExists(pipefn) {
				logger.Info("%s ok", pipefn)
			} else {
				logger.Info("creating fifo %s", pipefn)
				out, err := exec.Command("mkfifo", pipefn).CombinedOutput()
				if err != nil {
					logger.Error("mkfifo %s: %s\n%s", pipefn, err, out)
					pipefn = ""
				}
			}
			if pipefn != "" {
				c.Commands, err = pipeChan(pipefn)
				if err != nil {
					logger.Error("cannot open %s for channel: %s", pipefn, err)
					c.Commands = nil
				}
			}
		}
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

type ControlCommand int

const (
	INDEX_RESET ControlCommand = 1 << iota
)

type Controller chan ControlCommand

func (c Controller) IndexReset() {
	select {
	case c <- INDEX_RESET:
		logger.Info("sent RESET signal")
	default:
		logger.Warn("couldn't send RESET signal")
	}
}

func pipeChan(pipefn string) (Controller, error) {
	fh, err := os.OpenFile(pipefn, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	c := make(Controller, 1)
	go func() {
		defer fh.Close()
		r := bufio.NewReader(fh)
		for {
			line, _, err := r.ReadLine()
			logger.Info("Read %s from %s (%s)", line, fh, err)
			if err == nil {
				switch BytesToStr(line) {
				case "RESET":
					c.IndexReset()
				default:
					logger.Warn("Unknown command %s", line)
					fh.Write(StrToBytes("UNKNOWN\n"))
				}
			} else if err != io.EOF {
				logger.Error("error reading %s: %s", fh, err)
			}
		}
	}()
	return c, nil
}

func fileMode(fn string) os.FileMode {
	if fh, err := os.Open(fn); err == nil {
		if fi, err := fh.Stat(); err == nil {
			return fi.Mode()
		}
	}
	return 0
}

func fifoExists(pipefn string) bool {
	dh, err := os.Open(filepath.Dir(pipefn))
	if err != nil {
		logger.Error("cannot open directory of %s: %s", pipefn, err)
		return false
	}
	bn := filepath.Base(pipefn)
	var mode os.FileMode
	for {
		files, err := dh.Readdir(1024)
		if err == io.EOF {
			break
		}
		for _, fi := range files {
			if fi.Name() == bn {
				mode = fi.Mode()
				logger.Trace("mode=%v", mode)
				if mode&os.ModeNamedPipe == 0 {
					logger.Warn("command_pipe=%s, but that is not a pipe!", pipefn)
					return false
				} else {
					return true
				}
			}
		}
	}
	return false
}
