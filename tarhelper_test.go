//!/usr/bin/env go
package aostor

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	//"net/http"
	"crypto/rand"
	"log"
)

func TestCompress(c *testing.T) {
	var (
		sf  *os.File
		cfn string
		err error
	)
	if sf, err = os.Open("tarhelper_test.go"); err != nil {
		c.Fatalf("opening file: %s", err)
	}
	var s []byte
	if s, err = ioutil.ReadAll(sf); err != nil {
		c.Fatalf("reading %s: %s", sf, err)
	}
	c.Logf("read %d.", len(s))
	if _, err := sf.Seek(0, 0); err != nil {
		c.Fatalf("seeking %s: %s", sf, err)
	}
	if cfn, err = Compress(sf, "gz"); err != nil {
		c.Fatalf("compressing %s: %s", sf, err)
	}
	defer os.Remove(cfn)
	c.Log(fmt.Sprintf("cfn=%s", cfn))
	var df io.Reader
	if df, err = os.Open(cfn); err != nil {
		c.Fatalf("opening %s: %s", cfn, err)
	}
	var r io.Reader
	if r, err = gzip.NewReader(df); err != nil {
		c.Fatalf("opening %s: %s", df, err)
	}
	var d []byte
	if d, err = ioutil.ReadAll(r); err != nil {
		c.Fatalf("reading %s: %s", r, err)
	}
	if len(s) != len(d) {
		c.Fatalf("len(s)=%d len(d)=%d", len(s), len(d))
	}
	for i := 0; i < len(s); i += 1 {
		if s[i] != d[i] {
			c.Logf("%d: s=%s d=%s", i, s[i], d[i])
		}
	}
}

func TestAppendFile(c *testing.T) {
	tarfn := os.TempDir() + "/tarhelper_test.tar"
	fi, err := os.Stat("tarhelper_test.go")
	oldsize := fi.Size()
	info := Info{m: map[string]string{InfoPref + "Id": fmt.Sprintf("1234-%d", os.Getpid())}}
	i := os.Getpid()%10 + 1
	buf := make([]byte, i)
	var n int
	if n, err = rand.Read(buf); err != nil {
		log.Printf("cannot read random: %s", err)
	}
	info.Add("X-Gibberish", fmt.Sprintf("%x", buf[:n]))
	pos, err := AppendFile(tarfn, info, fi.Name(), "gzip")
	if err != nil {
		c.Fatalf("appending to %s: %s", tarfn, err)
	}
	log.Printf("pos=%d", pos)
	if fi, err = os.Stat(tarfn); err != nil {
		c.Fatalf("not exists? %s", err)
	}
	newsize := fi.Size()
	log.Printf("old=%d new=%d", oldsize, newsize)
	if newsize == 0 || oldsize >= newsize {
		//c.Fatal()
	}
}
