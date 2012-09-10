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
	"sync"
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
	// devnull  io.Writer
	closer   func()
	urandom	io.Reader
	payloadbuf = bytes.NewBuffer(nil)
	payload_lock = sync.Mutex{}
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
	errch := make(chan error, 1+*parallel)
	donech := make(chan int64, *parallel)
	upl := func(dump bool) {
		bp := int64(0)
		for i := 0; i < *N; i++ {
			payload, err := getPayload()
			if err != nil {
				errch <- fmt.Errorf("error getting payload(%d): %s", i, err)
				break
			}
			if err = checkedUpload(payload, dump && bp < 1); err != nil {
				errch <- fmt.Errorf("error uploading: %s", err)
				break
			}
			bp += int64(len(payload.encoded))
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
	urand, err := os.Open("/dev/urandom")
	if err != nil {
		return nil, err
	}
	urandom = bufio.NewReader(urand)

  	if url != "" {
		return nil, nil
	}

	flag.Parse()

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
	data    []byte
	encoded []byte
	ct      string
	length  uint64
}


func getPayload() (pLoad, error) {
	payload_lock.Lock()
	defer payload_lock.Unlock()
	n, err := io.CopyN(payloadbuf, urandom, 8)
	if err != nil {
		// payload_lock.Unlock()
		log.Panicf("cannot read %s: %s", urandom, err)
	}
	buf := payloadbuf.Bytes()
	length := len(buf)
	if length == 0 {
		log.Fatalf("zero payload (length=%d read=%d)", length, n)
	}
	reqbuf := bytes.NewBuffer(make([]byte, 0, 2*length+256))
	mw := multipart.NewWriter(reqbuf)
	w, err := mw.CreateFormFile("upfile", fmt.Sprintf("test-%d",length))
	if err != nil {
		log.Panicf("cannot create FormFile: %s", err)
	}
	m, err := w.Write(buf)
	if err != nil {
		log.Printf("written payload is %d bytes (%s)", m, err)
	}
	mw.Close()
	return pLoad{buf, reqbuf.Bytes(),
		mw.FormDataContentType(), uint64(length)}, nil
}

func checkedUpload(payload pLoad, dump bool) error {
	key, err := upload(payload, dump)
	if err != nil {
		return err
	}
	if key != nil {
		return get(key, payload)
	}
	return nil
}

func upload(payload pLoad, dump bool) ([]byte, error) {
	// log.Printf("body:\n%v", reqbuf)

	req, err := http.NewRequest("POST", url+"/up", bytes.NewReader(payload.encoded))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(payload.encoded))
	req.Header.Set("Content-Type", payload.ct)
	if dump {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Panicf("cannot dump request %v: %s", req, e)
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%v", buf)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Printf("cannot dump request %v: %s", req, e)
			return nil, nil
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
			log.Printf("cannot dump response %v: %s", resp, e)
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

func get(key []byte, payload pLoad) error {
	resp, err := http.Get(url + "/" + aostor.BytesToStr(key))
	if err != nil {
		return fmt.Errorf("error Getting %s: %s",
			url+"/"+aostor.BytesToStr(key), err)
	}
	defer resp.Body.Close()
	if !(200 <= resp.StatusCode && resp.StatusCode <= 299) {
		return fmt.Errorf("%s", resp.Status)
	}
	c := aostor.NewCounter()
	buf, err := ioutil.ReadAll(io.TeeReader(resp.Body, c))
	if err != nil {
		buf, e := httputil.DumpResponse(resp, true)
		if e != nil {
			log.Printf("cannot dump response %v: %s", resp, e)
		}
		return fmt.Errorf("error reading from %s: %s\n<<<<<<\nresponse:\n%v",
			resp.Body, err, buf)
	}
	if c.Num != payload.length {
		return fmt.Errorf("length mismatch: read %d bytes (%d content-length=%d) for %s, required %d\n%s",
			c.Num, len(buf), resp.ContentLength, key, payload.length, resp)
	}
	if payload.data != nil && uint64(len(payload.data)) == payload.length && !bytes.Equal(payload.data, buf) {
		return fmt.Errorf("data mismatch: read %v, asserted %v", buf, payload.data)
	}
	return nil
}
