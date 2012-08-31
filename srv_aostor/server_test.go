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
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
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
	urandom  io.Reader
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
	time.Sleep(1 * time.Second)
	bp := int64(0)
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		if err = checkedUpload(uint64(i), i < 2); err != nil {
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
	urandom = bufio.NewReader(urandom)
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

func checkedUpload(length uint64, dump bool) error {
	buf := bytes.NewBuffer(make([]byte, 0, length))
	n, err := io.CopyN(buf, urandom, int64(length))
	if err != nil {
		return err
	}
	// buf.Write([]byte{'\r', '\n'})
	b := buf.Bytes()
	log.Printf("read %d bytes from urandom, len(buf)=%d: %v", n, len(b), b)
	b2 := make([]byte, len(b))
	n2, e := io.ReadFull(bytes.NewReader(b), b2)
	log.Printf("reading back from %v: %v (%d - %s)", b, b2, n2, e)
	key, err := upload(bytes.NewReader(b), uint64(len(b)), dump)
	if err != nil {
		return err
	}
	return get(key, length)
}

func upload(payload io.Reader, length uint64, dump bool) ([]byte, error) {
	req, err := http.NewRequest("POST", url+"/up", payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", length))
	req.Header.Set("Content-Disposition",
		fmt.Sprintf("inline; filename=\"test-%d\"", length))
	resp, err := client.Do(req)
	if err != nil || dump {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Panicf("cannot dump request %s: %s", req, e)
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%s", buf)
		}
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	key := make([]byte, 32)
	n, err := resp.Body.Read(key)
	if err != nil || dump {
		buf, e := httputil.DumpResponse(resp, true)
		if e != nil {
			log.Printf("cannot dump response %s: %s", resp, e)
		} else {
			log.Printf("\n<<<<<<\nresponse:\n%s", buf)
		}
	}
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
	c := aostor.NewCounter()
	io.Copy(c, resp.Body)
	if c.Num != length || resp.ContentLength > -1 && int64(length) != resp.ContentLength {
		return fmt.Errorf("length mismatch: read %d bytes (content-length=%d) for %s, required %d",
			c.Num, resp.ContentLength, key, length)
	}
	return nil
}
