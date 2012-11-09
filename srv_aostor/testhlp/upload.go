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
package testhlp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
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
	urandom      io.Reader
	payloadbuf   = bytes.NewBuffer(nil)
	payload_lock = sync.Mutex{}
	shovelLock   = sync.Mutex{}
)

type Server struct {
	Url   string //url of server
	Pid   int    //pid of server process
	Close func() // closer
}

func OneRound(baseUrl string, parallel, N int, urlch chan<- string, dump bool) (err error) {
	errch := make(chan error, 1+parallel)
	donech := make(chan int64, parallel)
	upl := func(dump bool) {
		bp := int64(0)
		var url string
		for i := 0; i < N; i++ {
			payload, err := getPayload()
			if err != nil {
				errch <- fmt.Errorf("error getting payload(%d): %s", i, err)
				break
			}
			for j := rand.Int() % 15; j < 2; j++ {
				if url, err = CheckedUpload(baseUrl, payload, dump && bp < 1); err != nil {
					errch <- fmt.Errorf("error uploading: %s", err)
					break
				}
				bp += int64(len(payload.encoded))
				select {
				case urlch <- url:
				default:
				}
			}
		}
		donech <- bp
	}
	for j := 0; j < parallel; j++ {
		go upl(dump && j < 1)
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
	if hostport != "" {
		srv.Url = "http://" + hostport + "/test"
		return
	}
	c, e := aostor.ReadConf("", "test")
	if e != nil {
		err = e
		return
	}
	srv.Url = "http://" + c.Hostport + "/test"
	cmd := exec.Command("./srv_aostor")
	if err = cmd.Start(); err != nil {
		return
	}
	time.Sleep(1 * time.Second)
	srv.Pid = cmd.Process.Pid
	srv.Close = func() {
		aostor.FlushLog()
		cmd.Process.Kill()
	}
	return
}

func Shovel(pid int, hostport string) error {
	args := []string{"-r=test"}
	if pid > 0 {
		args = append(args, fmt.Sprintf("-p=%d", pid))
	}
	if hostport != "" {
		args = append(args, "-http="+hostport)
	}
	shovelLock.Lock()
	defer shovelLock.Unlock()
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
	n, err := io.CopyN(payloadbuf, urandom, 128)
	if err != nil {
		// payload_lock.Unlock()
		log.Panicf("cannot read %s: %s", urandom, err)
	}
	buf := payloadbuf.Bytes()
	length := len(buf)
	if length > 65 {
		payloadbuf.Write(buf[length-65:])
	}
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

func CheckedUpload(baseUrl string, payload PLoad, dump bool) (string, error) {
	key, err := Upload(baseUrl, payload, dump)
	if err != nil {
		return key.String(), err
	}
	if !key.IsEmpty() {
		var txt string
		for i := 0; i < 10; i++ {
			if txt, err = Get(baseUrl, key, payload); err == nil {
				return txt, nil
			}
			log.Printf("WARN[%d] cannot get %s: %s", i, key, err)
			time.Sleep(1)
		}
		return txt, err
	}
	return "", nil
}

func Upload(baseUrl string, payload PLoad, dump bool) (key aostor.UUID, err error) {
	// log.Printf("body:\n%v", reqbuf)

	req, e := http.NewRequest("POST", baseUrl+"/up", bytes.NewReader(payload.encoded))
	if e != nil {
		err = e
		return
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
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		buf, e := httputil.DumpRequestOut(req, true)
		if e != nil {
			log.Printf("cannot dump request %v: %s", req, e)
			return aostor.UUID{}, nil
		} else {
			log.Printf("\n>>>>>>\nrequest:\n%v", buf)
		}
	}
	if e != nil {
		err = e
		return
	}
	defer resp.Body.Close()
	buf := make([]byte, 32)
	n, e := resp.Body.Read(buf)
	if e != nil || dump {
		buf, e := httputil.DumpResponse(resp, true)
		if e != nil {
			log.Printf("cannot dump response %v: %s", resp, e)
		} else {
			log.Printf("\n<<<<<<\nresponse:\n%v", buf)
		}
	}
	if e == nil {
		key, e = aostor.UUIDFromBytes(buf[:n])
	}
	if e != nil {
		err = e
		return
	}
	if n != 2*aostor.UUIDLength || bytes.Equal(bytes.ToUpper(key[:3]), []byte{'E', 'R', 'R'}) {
		return aostor.UUID{}, fmt.Errorf("bad response: %s", key)
	}
	// log.Printf("%s", key)
	return
}

func Get(baseUrl string, key aostor.UUID, payload PLoad) (url string, err error) {
	url = baseUrl + "/" + key.String()
	resp, e := http.Get(url)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if e != nil {
		err = fmt.Errorf("error with http.Get(%s): %s", url, e)
		return
	}
	if !(200 <= resp.StatusCode && resp.StatusCode <= 299) {
		err = fmt.Errorf("STATUS=%s (%s)", resp.Status, url)
		return
	}
	c := aostor.NewCounter()
	buf, err := ioutil.ReadAll(io.TeeReader(resp.Body, c))
	if err != nil {
		buf, e := httputil.DumpResponse(resp, true)
		if e != nil {
			log.Printf("cannot dump response %v: %s", resp, e)
		}
		err = fmt.Errorf("error reading from %s: %s\n<<<<<<\nresponse:\n%v",
			resp.Body, err, buf)
		return
	}
	if c.Num != payload.length {
		err = fmt.Errorf("length mismatch: read %d bytes (%d content-length=%d) for %s, required %d\n%s",
			c.Num, len(buf), resp.ContentLength, key, payload.length, resp)
		return
	}
	if payload.data != nil && uint64(len(payload.data)) == payload.length && !bytes.Equal(payload.data, buf) {
		err = fmt.Errorf("data mismatch: read %v, asserted %v", buf, payload.data)
		return
	}
	return
}
