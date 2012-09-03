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
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"runtime"
	// "strings"
	"testing"
	"time"
	// "testing/iotest"
	"unosoft.hu/aostor"
)

var (
	parallel = flag.Int("P", 1, "parallel threads")
	hostport = flag.String("http", "", "already running server address")
	N        = flag.Int("N", 100, "request number")
	url      string
	devnull  io.Writer
	urandom  io.Reader
	closer   func()
)

func TestParallelStore(t *testing.T) {
	var err error
	closer, err = startServer()
	if err != nil {
		log.Panicf("error starting server: %s", err)
	}
	if closer != nil {
		defer closer()
	}
	log.Printf("parallel=%v (%d)", parallel, *parallel)
	payloadch, err := payloadGenerator(*N, *parallel)
	if err != nil {
		t.Fatalf("error starting payloadGenerator(%d, %d): %s", *N, *parallel, err)
	}
	errch := make(chan error, 1+*parallel)
	donech := make(chan int64, *parallel)
	upl := func(dump bool) {
		bp := int64(0)
		for payload := range payloadch {
			if err = checkedUpload(payload, dump && bp < 1); err != nil {
				errch <- fmt.Errorf("error uploading %s: %s", payload, err)
				break
			}
			bp += int64(len(payload.body))
		}
		donech <- bp
	}
	for j := 0; j < *parallel; j++ {
		go upl(j < 1)
	}
	gbp := int64(0)
	for i := 0; i < *parallel; {
		select {
		case err = <-errch:
			t.Fatalf("ERROR: %s", err)
		case b := <-donech:
			i++
			gbp += b
		}
	}
	log.Printf("done %d bytes", gbp)
}

func startServer() (func(), error) {
	if url != "" {
		return nil, nil
	}
	flag.Parse()
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

	log.Printf("hostport=%s = %s", hostport, *hostport)
	if *hostport == "" {
		c, err := aostor.ReadConf("", "test")
		if err != nil {
			return nil, err
		}
		url = "http://" + c.Hostport + "/test"
		cmd := exec.Command("./srv_aostor")
		err = cmd.Start()
		time.Sleep(1 * time.Second)
		return func() { cmd.Process.Kill() }, err
	}
	url = "http://" + *hostport + "/test"
	runtime.GOMAXPROCS(runtime.NumCPU())
	return nil, nil
}

type pLoad struct {
	body   []byte
	ct     string
	length uint64
}

func payloadGenerator(cnt int, mul int) (<-chan pLoad, error) {
	outch := make(chan pLoad, mul)

	go func() {
		length := int64(0)
		buf := bytes.NewBuffer(nil)
		for j := 0; j < cnt; j++ {
			n, err := io.CopyN(buf, urandom, 32 * (1<<10))
			if err != nil {
				log.Panicf("cannot read %s: %s", urandom, err)
			}
			length += n
			if len(buf.Bytes()) == 0 {
				log.Fatalf("zero payload (length=%d n=%d)", length, n)
			}
			reqbuf := bytes.NewBuffer(make([]byte, 0, 2*length+256))
			mw := multipart.NewWriter(reqbuf)
			w, err := mw.CreateFormFile("upfile", fmt.Sprintf("test-%d", j+1))
			if err != nil {
				log.Panicf("cannot create FormFile: %s", err)
			}
			m, err := w.Write(buf.Bytes())
			if err != nil {
				log.Printf("written payload is %d bytes (%s)", m, err)
			}
			mw.Close()
			payload := pLoad{reqbuf.Bytes(), mw.FormDataContentType(), uint64(length)}
			for i := 0; i < mul; i++ {
				outch <- payload
			}
		}
		close(outch)
	}()

	return outch, nil
}

func checkedUpload(payload pLoad, dump bool) error {
	key, err := upload(payload, dump)
	if err != nil {
		return err
	}
	return get(key, payload.length)
}

func upload(payload pLoad, dump bool) ([]byte, error) {
	// log.Printf("body:\n%v", reqbuf)

	req, err := http.NewRequest("POST", url+"/up", bytes.NewReader(payload.body))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(payload.body))
	req.Header.Set("Content-Type", payload.ct)
	if dump {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Panicf("cannot dump request %s: %s", req, e)
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%v", buf)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Panicf("cannot dump request %s: %s", req, e)
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%v", buf)
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
			log.Printf("\n<<<<<<\nresponse:\n%v", buf)
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
	if !(200 <= resp.StatusCode && resp.StatusCode <= 299) {
		return fmt.Errorf("%s", resp.Status)
	}
	c := aostor.NewCounter()
	buf, err := ioutil.ReadAll(io.TeeReader(resp.Body, c))
	if err != nil {
		return fmt.Errorf("error reading from %v: %s", resp.Body, err)
	}
	if c.Num != length {
		return fmt.Errorf("length mismatch: read %d bytes (%d content-length=%d) for %s, required %d\n%s",
			c.Num, len(buf), resp.ContentLength, key, length, resp)
	}
	return nil
}
