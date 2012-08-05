// Copyright 2012 Tamás Gulácsi, UNO-SOFT Computing Ltd.
// This file is part of aostor.

// Aostor is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Foobar is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Foobar.  If not, see <http://www.gnu.org/licenses/>.

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
	"unosoft.hu/aostor/compressor"
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
	if cfn, err = compressor.CompressToTemp(sf, "gz"); err != nil {
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
	tarfn, oldsize, info, fn, err := initAppend()
	if err != nil {
		c.Fatalf("cannot initialize: %s", err)
	}
	pos, err := AppendFile(tarfn, info, fn, "gzip")
	if err != nil {
		c.Fatalf("appending to %s: %s", tarfn, err)
	}
	log.Printf("pos=%d", pos)
	fi, err := os.Stat(tarfn)
	if err != nil {
		c.Fatalf("not exists? %s", err)
	}
	newsize := fi.Size()
	log.Printf("old=%d new=%d", oldsize, newsize)
	if newsize == 0 || oldsize >= newsize {
		//c.Fatal()
	}
}

func initAppend() (tarfn string, oldsize int64, info Info, fn string, err error) {
	tarfn = os.TempDir() + "/tarhelper_test.tar"
	fi, err := os.Stat("tarhelper_test.go")
	oldsize = fi.Size()
	uuid, err := StrUUID()
	if err != nil {
		fmt.Printf("cannot generate uuid: %s", err)
		return
	}
	info = Info{m: map[string]string{InfoPref + "Id": uuid}}
	i := os.Getpid()%10 + 1
	buf := make([]byte, i)
	var n int
	if n, err = rand.Read(buf); err != nil {
		log.Printf("cannot read random: %s", err)
	}
	info.Add("X-Gibberish", fmt.Sprintf("%x", buf[:n]))
	return
}

func BenchmarkAppendFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tarfn, _, info, fn, err := initAppend()
		if err != nil {
			b.Fatalf("cannot initialize: %s", err)
		}
		b.StartTimer()
		_, err = AppendFile(tarfn, info, fn, "gzip")
		if err != nil {
			b.Fatalf("appending to %s: %s", tarfn, err)
		}
	}
}
