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

import _ "net/http/pprof" // pprof

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
	"github.com/tgulacsi/aostor"
)

var logger = log.New(os.Stderr, "server ", log.LstdFlags|log.Lshortfile)
var MaxRequestMemory = 20 * int64(1<<20)

func main() {
	defer aostor.FlushLog()
	configfile := flag.String("c", aostor.ConfigFile, "config file")
	hostport := flag.String("http", "",
		"host:port, default="+aostor.DefaultHostport)
	flag.Parse()
	conf, err := aostor.ReadConf(*configfile, "")
	if err != nil {
		logger.Fatalf("cannot read configuration %s: %s", *configfile, err)
	} else {
		aostor.ConfigFile = *configfile
		logger.Printf("set configfile: %s", aostor.ConfigFile)
	}
	if *hostport != "" {
		conf.Hostport = *hostport
	}

	s := prepareServer(&conf)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGUSR1)

	go recvChangeSig(sigchan)

	runtime.GOMAXPROCS(runtime.NumCPU())
	// runtime.GOMAXPROCS(1)

	logger.Printf("starting server on %s", *s)
	logger.Fatal(s.ListenAndServe())
}

func prepareServer(conf *aostor.Config) *http.Server {
	http.HandleFunc("/", indexHandler)
	for _, realm := range conf.Realms {
		http.HandleFunc("/"+realm+"/", baseHandler)
		http.HandleFunc("/"+realm+"/up", upHandler)
	}
	http.HandleFunc("/_signal", sigHandler)

	s := &http.Server{
		Addr:           conf.Hostport,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1Mb
	}
	return s
}

func recvChangeSig(sigchan <-chan os.Signal) {
	// aostor.FillCaches(true)
	for _ = range sigchan {
		logger.Printf("\n\n***\nreceived Change signal, calling FillCaches")
		aostor.FillCaches(true)
		logger.Printf("\n***\n\n")
	}
}
func sigHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("SIGNAL")
	aostor.FillCaches(true)
	w.Write([]byte("OK"))
}

func baseHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("base got %s", r)
	tmp := strings.SplitN(r.URL.Path, "/", 3)[1:]
	realm, path := tmp[0], tmp[1]
	logger.Printf("realm=%s path=%s", realm, path)

	if r.Method == "GET" || r.Method == "HEAD" {
		key, err := aostor.NewUUIDFromString(path)
		if err != nil {
			http.Error(w, fmt.Sprintf("404 Bad key %s", path), 404)
			return
		}
		info, data, err := aostor.Get(realm, key)
		if err != nil {
			logger.Print(err)
			http.Error(w, fmt.Sprintf("404 Page Not Found (%s): %s", path, err), 404)
		} else if !(info.Key != "" && data != nil) {
			logger.Printf("NULL answer")
			http.Error(w, fmt.Sprintf("404 Page Not Found (%s)", path), 404)
		} else {
			info.Copy(w.Header())
			//logger.Printf("copying from %s to %s", data, err)
			n, err := io.Copy(w, data)
			if err != nil {
				logger.Printf("Error copying from %s to %s: %s", data, w, err)
			} else {
				logger.Printf("written %d bytes", n)
			}
			return
		}
	} else if r.Method == "POST" {
		r.URL.Path = "/" + realm + "/up/" + path
		upHandler(w, r)
	} else {
		http.Error(w, fmt.Sprintf("400 Bad Request: unknown method %s", r.Method), 400)
	}
	return
}

func upHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("work failed:", err)
			panic(fmt.Sprintf("work failed: %s", err))
		}
	}()
	logger.Printf("got %s", r)
	tmp := strings.SplitN(r.URL.Path, "/", 3)[1:]
	realm, path := tmp[0], tmp[1]
	logger.Printf("realm=%s path=%s", realm, path)
	if r.Method != "POST" {
		http.Error(w, fmt.Sprintf("400 Bad Request: unknown method %s", r.Method), 400)
		return
	}
	tmp = strings.SplitN(r.URL.Path, "/", 4)[1:]
	realm, up, path := tmp[0], "up", ""
	if len(tmp) > 1 {
		up = tmp[1]
	}
	if len(tmp) > 2 {
		path = tmp[2]
	}
	ct := r.Header.Get("Content-Type")
	p := strings.Index(ct, ";")
	if p >= 0 {
		ct = ct[:p]
	}
	logger.Printf("realm=%s path=%s up=%s content-type=%s", realm, path, up, ct)
	if up != "up" {
		http.Error(w, "400 Bad Request: unknown path "+up, 400)
		return
	}
	var (
		err      error
		file     io.Reader
		headers  map[string][]string
		filename string
	)
	if ct == "multipart/form-data" || ct == "application/x-www-form-urlencoded" {
		if err = r.ParseMultipartForm(MaxRequestMemory); err != nil {
			http.Error(w, fmt.Sprintf("400 Bad Request: cannot parse as multipart"), 400)
			return
		}
		if r.MultipartForm == nil || r.MultipartForm.File == nil || 0 == len(r.MultipartForm.File) {
			http.Error(w, fmt.Sprintf("400 Bad Request: no file in POST upload"), 400)
			return
		}
		k := "upfile"
		if _, ok := r.MultipartForm.File[k]; !ok {
			for k = range r.MultipartForm.File {
				break
			}
		}
		f, hdr, e := r.FormFile(k)
		if e != nil {
			logger.Printf("error getting upfile from form: %s", e)
			err = e
		} else {
			file = f
			headers = hdr.Header
			ct = hdr.Header.Get("Content-Type")
			filename = hdr.Filename
			logger.Printf("FORM f=%v headers=%s ct=%s", file, headers, ct)
		}
	} else {
		file = base64.NewDecoder(base64.URLEncoding, r.Body)
		headers = r.Header
		// Content-Disposition: attachment; filename="inline; filename="test-67""
		// ->
		// test-67
		filename = r.Header.Get("Content-Disposition")
		p = strings.LastIndex(filename, "filename=")
		if p >= 0 {
			filename = strings.Trim(filename[p+9:], ` "'`)
		}
		logger.Printf("RAW f=%v headers=%s ct=%s", file, headers, ct)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("400 Bad Request: upfile missing: %s", err), 400)
		return
	}
	info := aostor.Info{}
	info.CopyFrom(headers)
	info.SetFilename(filename, ct)
	logger.Printf("filename: %s info: %s", filename, info)
	fbuf := bufio.NewReader(file)
	if _, e := fbuf.Peek(1); e != nil {
		http.Error(w, fmt.Sprintf("400 Bad Request: empty body"), 400)
		return
	}
	key, err := aostor.Put(realm, info, fbuf)
	if err != nil {
		http.Error(w, fmt.Sprintf("ERROR: %s", err), 500)
		return
	}
	w.Header().Add(aostor.InfoPref+"Key", key)
	w.Write(aostor.StrToBytes(key))
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("got %s", r)
}

// _signal panics with tip, 
// The first bad revision is:
// changeset:   14713:bb4ee132b967
// user:        Luuk van Dijk <lvd@golang.org>
// date:        Mon Oct 29 13:55:27 2012 +0100
// summary:     cmd/gc: inlining functions with local variables
