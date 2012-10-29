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
	"time"
	// "testing/iotest"
	"unosoft.hu/aostor"
)

var (
	url string
	// devnull  io.Writer
	closer       func()
	urandom      io.Reader
	payloadbuf   = bytes.NewBuffer(nil)
	payload_lock = sync.Mutex{}
)

type Server struct {
	Url   string //url of server
	Pid   int    //pid of server process
	Close func() // closer
}

// if called from command-line, start the server and push it under load!
func main() {
	hostport := flag.String("http", "", "already running server's address0")
	flag.Parse()
	srv, err := StartServer(*hostport)
	if err != nil {
		log.Panicf("error starting server: %s", err)
	}
	if closer != nil {
		defer closer()
	}

	for i := 1; i < 100; i++ {
		log.Printf("starting round %d...", i)
		if err = OneRound(i, 1000); err != nil {
			log.Printf("error with round %d: %s", err)
			break
		}
		log.Printf("starting shovel of %d...", i)
		if err = Shovel(srv.Pid); err != nil {
			log.Printf("error with shovel: %s", err)
			break
		}
	}
}

func OneRound(parallel, N int) (err error) {
	errch := make(chan error, 1+parallel)
	donech := make(chan int64, parallel)
	upl := func(dump bool) {
		bp := int64(0)
		for i := 0; i < N; i++ {
			payload, err := getPayload()
			if err != nil {
				errch <- fmt.Errorf("error getting payload(%d): %s", i, err)
				break
			}
			if err = CheckedUpload(payload, dump && bp < 1); err != nil {
				errch <- fmt.Errorf("error uploading: %s", err)
				break
			}
			bp += int64(len(payload.encoded))
		}
		donech <- bp
	}
	for j := 0; j < parallel; j++ {
		go upl(j < 1)
	}
	gbp := int64(0)
	for i := 0; i < parallel; {
		select {
		case err = <-errch:
			log.Printf("ERROR: %s", err)
			return
		case b := <-donech:
			i++
			gbp += b
		}
	}
	log.Printf("done %d bytes", gbp)
	return nil
}

func StartServer(hostport string) (srv Server, err error) {
	if urandom == nil {
		var urand io.Reader
		urand, err = os.Open("/dev/urandom")
		if err != nil {
			return
		}
		urandom = bufio.NewReader(urand)
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	log.Printf("hostport=%s", hostport)
	if hostport == "" {
		c, e := aostor.ReadConf("", "test")
		if e != nil {
			err = e
			return
		}
		url = "http://" + c.Hostport + "/test"
		cmd := exec.Command("./srv_aostor")
		if err = cmd.Start(); err != nil {
			return
		}
		time.Sleep(1 * time.Second)
		srv.Pid = cmd.Process.Pid
		srv.Close = func() { cmd.Process.Kill() }
	}
	srv.Url = "http://" + hostport + "/test"
	return
}

func Shovel(pid int) error {
	args := []string{"-r=test"}
	if pid > 0 {
		args = append(args, fmt.Sprintf("-p=%d", pid))
	}
	cmd := exec.Command("./shovel", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type PLoad struct {
	data    []byte
	encoded []byte
	ct      string
	length  uint64
}

func getPayload() (PLoad, error) {
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
	w, err := mw.CreateFormFile("upfile", fmt.Sprintf("test-%d", length))
	if err != nil {
		log.Panicf("cannot create FormFile: %s", err)
	}
	m, err := w.Write(buf)
	if err != nil {
		log.Printf("written payload is %d bytes (%s)", m, err)
	}
	mw.Close()
	return PLoad{buf, reqbuf.Bytes(),
		mw.FormDataContentType(), uint64(length)}, nil
}

func CheckedUpload(payload PLoad, dump bool) error {
	key, err := Upload(payload, dump)
	if err != nil {
		return err
	}
	if key != nil {
		return Get(key, payload)
	}
	return nil
}

func Upload(payload PLoad, dump bool) ([]byte, error) {
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

func Get(key []byte, payload PLoad) error {
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
