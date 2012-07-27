package aostor

import (
	"testing"
	"os"
	)

func initConfig() {
	logger.Printf("config: %s exists? %s", ConfigFile, fileExists(ConfigFile))
	if !fileExists(ConfigFile) {
		fh, err := os.OpenFile(ConfigFile, os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			panic(err)
		}
		_, err = fh.WriteString("[dirs]\nstaging = /tmp/aostor/staging\nindex = /tmp/aostor/ndx\n\n[index]\nthreshold = 2")
		if err != nil {
			panic(err)
		}
		fh.Close()
	}
}

func testPut() (string, error) {
	info := Info{}
	fn := "store_test.go"
	data, err := os.Open(fn)
	if err != nil {
		logger.Printf("cannot open %s: %s", fn, err)
		return "", err
	}
	return Put(info, data)
}

func TestPut(c *testing.T) {
	initConfig()
	if _, err := testPut(); err != nil {
		c.Fatalf("error on put: %s", err)
	}
}

func TestGet(c *testing.T) {
	initConfig()
	key, err := testPut()
	if err != nil {
		c.Fatalf("cannot put: %s", err)
	}
	info, _, err := Get(key)
	if err != nil {
		c.Fatalf("cannot get %s: %s", key, err)
	}
	if info.Key != key {
		c.Fatalf("key mismatch: asked for %s, got %s", key, info.Key)
	}
}