//!/usr/bin/env go
package aodb

import (
	"fmt"
	"testing"
	"io"
	"io/ioutil"
	"os"
	"compress/gzip"
	)

func TestCompress(c *testing.T) {
	var (sf *os.File
		cfn string
		err error)
	if sf, err = os.Open("tarhelper_test.go"); err != nil {
		c.Fatalf("opening file: %s", err)
	}
	var s []byte
	if s, err = ioutil.ReadAll(sf); err != nil {
		c.Fatalf("reading %s: %s", sf, err)
	}
	if _, err := sf.Seek(0, 0); err != nil {
		c.Fatalf("seeking %s: %s", sf, err)
	}
	if cfn, err = Compress(sf, "gz"); err != nil {
		c.Fatalf("compressing %s: %s", sf, err)
	}
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