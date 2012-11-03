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

package main

import (
	"../testhlp"
	"flag"
	"log"
	"net/http"
	"os"
	"time"
	"unosoft.hu/aostor"
)

// if called from command-line, start the server and push it under load!
func main() {
	defer aostor.FlushLog()
	hostport := flag.String("http", "", "already running server's address0")
	stage_interval := flag.Int("stage", 5, "stage interval (seconds)")
	flag.Parse()
	srv, err := testhlp.StartServer(*hostport)
	if err != nil {
		log.Panicf("error starting server: %s", err)
	}
	defer func() {
		if srv.Close != nil {
			srv.Close()
		}
	}()

	urlch := make(chan string, 1000)
	defer close(urlch)
	go func(urlch <-chan string) {
		for url := range urlch {
			resp, e := http.Get(url)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			if e != nil {
				log.Printf("error with http.Get(%s): %s", url, e)
				os.Exit(1)
			}
			if !(200 <= resp.StatusCode && resp.StatusCode <= 299) {
				log.Printf("STATUS=%s for %s", resp.Status, url)
				os.Exit(2)
			}
		}
	}(urlch)
	if *stage_interval > 0 {
		ticker := time.Tick(time.Duration(*stage_interval) * time.Second)
		// defer close(ticker)
		go func(ch <-chan time.Time, hostport string) {
			for now := range ch {
				log.Printf("starting shovel at %s...", now)
				if err = testhlp.Shovel(srv.Pid, hostport); err != nil {
					log.Printf("error with shovel: %s", err)
					break
				}
			}
		}(ticker, *hostport)
	}
	for i := 4; i < 100; i++ {
		log.Printf("starting round %d...", i)
		if err = testhlp.OneRound(srv.Url, i, 100, urlch, i == 1); err != nil {
			log.Printf("error with round %d: %s", i, err)
			break
		}
	}
}
