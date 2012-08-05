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
	"flag"
	"fmt"
	"unosoft.hu/aostor"
)

func main() {
	defer aostor.FlushLog()
	flag.Parse()
	tarfn, dirname := flag.Arg(0), flag.Arg(1)
	if err := aostor.CreateTar(tarfn, dirname, true); err != nil {
		fmt.Printf("ERROR: %s", err)
	} else {
		fmt.Println("OK")
	}
}
