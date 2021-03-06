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

import _ "net/http/pprof"

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	// "runtime/pprof"
	"syscall"
	"unosoft.hu/aostor"
)

var logger = aostor.GetLogger()
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func main() {
	defer aostor.FlushLog()
	var pid int
	var hostport string
	var changed bool
	flag.IntVar(&pid, "p", 0, "pid to SIGUSR1 on change")
	flag.StringVar(&hostport, "http", "", "host:port")
	todo_tar := flag.Bool("t", false, "shovel tar to dir")
	todo_realm := flag.String("r", "", "compact realm")
	flag.Parse()

	var onChange aostor.NotifyFunc
	if pid > 0 {
		process, err := os.FindProcess(pid)
		if hostport != "" {
			onChange = func() {
				resp, _ := http.Get("http://" + hostport + "/_signal")
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
			}
		}
		if err != nil {
			logger.Warn("cannot find pid %d: %s", pid, err)
		} else {
			oco := onChange
			onChange = func() {
				process.Signal(syscall.SIGUSR1)
				if oco != nil {
					oco()
				}
			}
		}

	}
	oco := onChange
	onChange = func() {
		changed = true
		if oco != nil {
			oco()
		}
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6500", nil))
	}()

	if *todo_tar {
		tarfn, dirname := flag.Arg(0), flag.Arg(1)
		if err := aostor.CreateTar(tarfn, dirname, 0, false); err != nil {
			fmt.Printf("ERROR shoveling %s to %s: %s", tarfn, dirname, err)
		} else {
			fmt.Println("OK")
			onChange()
		}
	} else if *todo_realm != "" {
		realm := *todo_realm
		if err := aostor.Compact(realm, onChange); err != nil {
			fmt.Printf("ERROR compacting %s: %s", realm, err)
		} else {
			if changed {
				fmt.Println("no change")
			} else {
				fmt.Println("OK")
			}
		}
	} else {
		fmt.Printf(`Usage:
prg -t tar dir [-p pid]
  or
prg -r realm [-p pid]
`)
	}

	// if *memprofile != "" {
	// 	f, err := os.Create(*memprofile)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	pprof.WriteHeapProfile(f)
	// 	f.Close()
	// }
}
