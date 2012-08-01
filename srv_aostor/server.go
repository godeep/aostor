// Append-Only Storage HTTP server
package main

import (
	"net/http"
	"unosoft.hu/aostor"
	"log"
	"flag"
	"os"
	"io"
	"fmt"
	"time"
	"strings"
	)

var logger = log.New(os.Stderr, "server ", log.LstdFlags|log.Lshortfile)

func main() {
	configfile := flag.String("c", aostor.ConfigFile, "config file")
	hostport := flag.String("hostport", "",
		"host:port, default=" + aostor.DefaultHostport)
	flag.Parse()
	conf, err := aostor.ReadConf(*configfile, "")
	if err != nil {
		logger.Fatalf("cannot read configuration %s: %s", *configfile, err)
	} else {
		aostor.ConfigFile = *configfile
		logger.Printf("set configfile: %s", aostor.ConfigFile)
	}

	http.HandleFunc("/", indexHandler)
	for _, realm := range(conf.Realms) {
		http.HandleFunc("/" + realm + "/", baseHandler)
		http.HandleFunc("/" + realm + "/up", upHandler)
	}

	if *hostport == "" {
		hostport = &conf.Hostport
	}
	s := &http.Server{
		Addr: *hostport,
		ReadTimeout: 30 * time.Second,
		WriteTimeout: 60 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1Mb
	}
	logger.Printf("starting server on %s", *s)
	logger.Fatal(s.ListenAndServe())
}

func baseHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("base got %s", r)
	tmp := strings.SplitN(r.URL.Path, "/", 3)[1:]
	realm, path := tmp[0], tmp[1]
	logger.Printf("realm=%s path=%s", realm, path)

	if r.Method == "GET" || r.Method == "HEAD" {
		info, data, err := aostor.Get(realm, path)
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
		}
	} else if r.Method == "POST" {
		r.URL.Path = "/" + realm + "/up/" + path
		upHandler(w, r)
	} else {
		http.Error(w, fmt.Sprintf("403 Bad Request: unknown method %s", r.Method), 403)
	}
	return
}

func upHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("got %s", r)
	tmp := strings.SplitN(r.URL.Path, "/", 3)[1:]
	realm, path := tmp[0], tmp[1]
	logger.Printf("realm=%s path=%s", realm, path)
	if r.Method == "POST" {
		tmp := strings.SplitN(r.URL.Path, "/", 4)[1:]
		realm, up, path := tmp[0], "up", ""
		if len(tmp) > 1 {
			up = tmp[1]
		}
		if len(tmp) > 2 {
			path = tmp[2]
		}
		logger.Printf("realm=%s path=%s up=%s", realm, path, up)
		if up != "up" {
			http.Error(w, "403 Bad Request: unknown path " + up, 403)
		} else {
			file, header, err := r.FormFile("upfile")
			info := aostor.Info{}
			info.CopyFrom(header.Header)
			info.SetFilename(header.Filename, header.Header.Get("Content-Type"))
			if err != nil {
				http.Error(w, fmt.Sprintf("403 Bad Request: upfile missing: %s", err), 403)
			} else {
				key, err := aostor.Put(realm, info, file)
				if err != nil {
					http.Error(w, fmt.Sprintf("ERROR: %s", err), 500)
				} else {
					w.Header().Add(aostor.InfoPref + "Key", key)
					w.Write(aostor.StrToBytes(key))
				}
			}
		}
	} else {
		http.Error(w, fmt.Sprintf("403 Bad Request: unknown method %s", r.Method), 403)
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("got %s", r)
}
