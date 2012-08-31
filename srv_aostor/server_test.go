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
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	// "strings"
	"testing"
	"time"
	// "testing/iotest"
	"unosoft.hu/aostor"
)

var (
	parallel = flag.Int("parallel", 1, "parallel threads")
	hostport = flag.String("http", "", "already running server address")
	url      string
	devnull  io.Writer
	urandom  io.Reader
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
	if close != nil {
		defer close()
	}
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
	// go main()

	if *hostport == "" {
		c, err := aostor.ReadConf("", "test")
		if err != nil {
			return nil, err
		}
		url = "http://" + c.Hostport + "/test"
		cmd := exec.Command("./srv_aostor")
		time.Sleep(1 * time.Second)
		return func() { cmd.Process.Kill() }, cmd.Start()
	}
	url = "http://" + *hostport + "/test"
	return nil, nil
}

func checkedUpload(length uint64, dump bool) error {
	buf := make([]byte, int(length))
	n, err := urandom.Read(buf)
	if err != nil {
		log.Printf("read %s (%d): %s", buf, n, err)
		return err
	}
	key, err := upload(buf, dump)
	if err != nil {
		return err
	}
	return get(key, length)
}

func upload(payload []byte, dump bool) ([]byte, error) {
	reqbuf := bytes.NewBuffer(make([]byte, 0, 2*len(payload)+1024))
	mw := multipart.NewWriter(reqbuf)
	w, err := mw.CreateFormFile("upfile", fmt.Sprintf("test-%d", len(payload)))
	if err != nil {
		return nil, err
	}
	n, err := w.Write(payload)
	if err != nil {
		log.Printf("written payload is %d bytes (%s)", n, err)
		return nil, err
	}
	mw.Close()
	// log.Printf("body:\n%v", reqbuf)

	req, err := http.NewRequest("POST", url+"/up", reqbuf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if dump {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Panicf("cannot dump request %s: %s", req, e)
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%s", buf)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
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
	n, err = resp.Body.Read(key)
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
	if !(200 <= resp.StatusCode && resp.StatusCode <= 299) {
		return fmt.Errorf("%s", resp.Status)
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
