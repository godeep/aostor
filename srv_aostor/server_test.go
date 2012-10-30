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
	"./testhlp"
	"flag"
	"testing"
	"unosoft.hu/aostor"
)

var (
	parallel = flag.Int("P", 1, "parallel threads")
	hostport = flag.String("http", "", "already running server address")
	N        = flag.Int("N", 1000, "request number")
)

func TestParallelStore(t *testing.T) {
	defer aostor.FlushLog()
	flag.Parse()
	srv, err := testhlp.StartServer(*hostport)
	if err != nil {
		t.Fatalf("error starting server: %s", err)
	}
	if srv.Close != nil {
		defer srv.Close()
	}
	t.Logf("parallel=%v (%d)", parallel, *parallel)
	urlch := make(chan string)
	if err = testhlp.OneRound(srv.Url, *parallel, *N, urlch, true); err != nil {
		t.Errorf("error while uploading: %s", err)
	}
	if err = testhlp.Shovel(srv.Pid, *hostport); err != nil {
		t.Errorf("error with shovel: %s", err)
	}
}
