package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"log"
	"os"
	"strings"
)

var logger = log.New(os.Stderr, "tarhelper ", log.LstdFlags|log.Lshortfile)

func main() {
	flag.Parse()
	tarfn, dirname := flag.Arg(0), flag.Arg(1)
	if err := aostor.CreateTar(tarfn, dirname); err != nil {
		fmt.Printf("ERROR: %s", err)
	} else {
		fmt.Println("OK")
	}
}
