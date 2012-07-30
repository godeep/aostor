package main

import (
	"flag"
	"log"
	"os"
	"unosoft.hu/aostor"
)

var logger = log.New(os.Stderr, "tarhelper ", log.LstdFlags|log.Lshortfile)

func main() {
	flag.Parse()
	tarfn, dirname := flag.Arg(0), flag.Arg(1)
	if err := aostor.CreateTar(tarfn, dirname, true); err != nil {
		logger.Printf("ERROR: %s", err)
	} else {
		logger.Println("OK")
	}
}
