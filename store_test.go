package aostor

import (
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"math/rand"
	"os"
	"testing"
)

func initConfig() {
	checkMerge = true
	if !fileExists(ConfigFile) {
		logger.Printf("config: %s not exists?", ConfigFile)
		fh, err := os.OpenFile(ConfigFile, os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			panic(err)
		}
		_, err = fh.WriteString(TestConfig)
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
	return Put("test", info, data)
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
	info, _, err := Get("test", key)
	if err != nil {
		c.Fatalf("cannot get %s: %s", key, err)
	}
	if info.Key != key {
		c.Fatalf("key mismatch: asked for %s, got %s", key, info.Key)
	}
}

func TestCompact(c *testing.T) {
	conf, err := ReadConf("", "test")
	if err != nil {
		c.Fatalf("cannot read conf: %s", err)
	}
	for j := uint(0); j < conf.IndexThreshold; j++ {
		for i := 0; i < 1000+rand.Intn(100); i++ {
			if _, err := testPut(); err != nil {
				c.Fatalf("cannot put: %s", err)
			}
		}
		//logger.Printf("MAX_CDB_SIZE: %d", MAX_CDB_SIZE)
		if err := CompactStaging("test"); err != nil {
			c.Fatalf("compact staging error: %s", err)
		}
	}
	if err := CompactIndices("test", 0); err != nil {
		c.Fatalf("compact indices error: %s", err)
	}
}

func TestCdbMerge(c *testing.T) {
	const N = 3
	filenames := make([]string, 2)
	filenames[0] = "/tmp/aostor_store_test-1.cdb"
	filenames[1] = "/tmp/aostor_store_test-2.cdb"
	keys := make([]string, 2*N)
	cw, err := cdb.NewWriter(filenames[0])
	if err != nil {
		c.Fatalf("cannot open %s: %s", filenames[0], err)
	}
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%d", i)
		cw.PutPair(StrToBytes(k), StrToBytes(filenames[0]))
		keys[i] = k
	}
	cw.Close()

	cw, err = cdb.NewWriter(filenames[1])
	if err != nil {
		c.Fatalf("cannot open %s: %s", filenames[1], err)
	}
	for i := N; i < 2*N; i++ {
		k := fmt.Sprintf("%d", i)
		cw.PutPair(StrToBytes(k), StrToBytes(filenames[1]))
		keys[i] = k
	}
	cw.Close()

	sfn := "/tmp/aostor_store_test.cdb"
	err = mergeCdbs(sfn, filenames, uint(0), uint(1), true)
	if err != nil {
		c.Fatalf("error merging %s: %s", filenames, err)
	}

	found := make([]string, 0)
	sfh, err := os.Open(sfn)
	if err != nil {
		c.Fatalf("cannot open %s: %s", sfn, err)
	}
	defer sfh.Close()
	cr := make(chan cdb.Element, 1)
	go cdb.DumpToChan(cr, sfh)
	for {
		elt, ok := <-cr
		if !ok {
			break
		}
		//logger.Printf("elt: %s", elt)
		if elt.Key[0] != '/' {
			found = append(found, BytesToStr(elt.Key))
		}
	}
	if fmt.Sprintf("%+v", found) != fmt.Sprintf("%+v", keys) {
		c.Fatalf("found=%s != keys=%s", found, keys)
	}
}
