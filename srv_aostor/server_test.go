// Copyright 2012 Tamás Gulácsi, UNO-SOFT Computing Ltd.
// This file is part of aostor.

// Aostor is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Aostor is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Aostor.  If not, see <http://www.gnu.org/licenses/>.

// Append-Only Storage HTTP server
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
	// "testing/iotest"
	"unosoft.hu/aostor"
)

var (
	parallel = flag.Int("parallel", 1, "parallel threads")
	url      string
	devnull  io.Writer
	urandom  io.ReadCloser
	client   = &http.Client{}
	close    func()
)

func init() {
	flag.Parse()
}

func BenchmarkStore(b *testing.B) {
	var err error
	b.StopTimer()
	close, err = startServer()
	if err != nil {
		log.Panicf("error starting server: %s", err)
	}
	defer close()
	time.Sleep(2 * time.Second)
	bp := int64(0)
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		if err = checkedUpload(uint64(i)); err != nil {
			b.Fatalf("error uploading %d: %s", i, err)
		}
		bp += int64(i)
		b.SetBytes(bp)
	}
}

func startServer() (func(), error) {
	var err error
	urandom, err = os.Open("/dev/urandom")
	if err != nil {
		return nil, err
	}
	devnull, err = os.OpenFile("/dev/null", os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	c, err := aostor.ReadConf("", "test")
	if err != nil {
		return nil, err
	}
	url = "http://" + c.Hostport + "/test"
	// go main()

	cmd := exec.Command("./srv_aostor")

	return func() {cmd.Process.Kill()}, cmd.Start()
}

func checkedUpload(length uint64) error {
	payload := io.LimitReader(urandom, int64(length))
	key, err := upload(payload.(*io.LimitedReader))
	if err != nil {
		return err
	}
	return get(key, length)
}

func upload(payload *io.LimitedReader) ([]byte, error) {
	req, err := http.NewRequest("POST", url+"/up", payload)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-Length", fmt.Sprintf("%d", payload.N))
	req.Header.Add("Content-Disposition",
		fmt.Sprintf("inline; filename=\"test-%d\"", payload.N))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	key := make([]byte, 32)
	n, err := resp.Body.Read(key)
	if err != nil {
		return nil, err
	}
	if n != 32 || bytes.Equal(bytes.ToUpper(key[:3]), []byte{'E', 'R', 'R'}) {
		return nil, fmt.Errorf("bad response: %s", key)
	}
	return key, nil
}

func get(key []byte, length uint64) error {
	resp, err := http.Get(url + "/" + aostor.BytesToStr(key))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c := new(counter)
	io.Copy(c, resp.Body)
	if c.n != length || resp.ContentLength > -1 && int64(length) != resp.ContentLength {
		return fmt.Errorf("length mismatch: read %d bytes (content-length=%d) for %s, required %d",
			c.n, resp.ContentLength, key, length)
	}
	return nil
}

// for counting written bytes
type counter struct {
	n uint64
}

func (c *counter) Write(p []byte) (n int, err error) {
	n = len(p)
	log.Printf("read %d(%s)", n, p)
	c.n += uint64(n)
	return n, nil
}
