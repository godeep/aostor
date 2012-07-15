package main

import (
	"github.com/jbarham/go-cdb"
	"unosoft.hu/aostor/tarhelper"
	"flag"
	"strings"
	"archive/tar"
	"fmt"
	"os"
	"io"
	)

type fElt struct {
	info aostor.Info
	infoFn string
	dataFn string
}

func CreateTar(tarfn string, dirname string) error {
	dh, err := os.Open(dirname)
	if err != nil {
		return err
	}
	defer dh.Close()
	list, err := dh.Readdir(-1)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(tarfn, os.O_APPEND|os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	defer fh.Close()
	tw := tar.NewWriter(fh)
	defer tw.Close()

	cfh, err := os.OpenFile(tarfn + ".cdb", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	ir, iw := io.Pipe()
	go cdb.Make(cfh, ir)

	var (
		buf map[string]fElt
		key string
		isInfo bool
		)
	for _, file := range list {
		nm := file.Name()
		switch {
		case strings.HasSuffix(nm, aostor.SuffInfo):
			key, isInfo = nm[:-1], true
		case strings.Contains(nm, aostor.SuffLink):
			key, isInfo = strings.Split(nm, aostor.SuffLink)[0], false
		case strings.Contains(nm, aostor.SuffData):
			key, isInfo = strings.Split(nm, aostor.SuffData)[0], false
		default:
			key, isInfo = "", true
		}
		if key != "" {
			elt, ok := buf[key]
			if isInfo {
				elt.infoFn = nm
			} else {
				elt.dataFn = nm
			}
			if ok && elt.infoFn != "" && elt.dataFn != "" { //full
				elt.info.Key = key
				elt.info.Ipos, err = appendFile(tw, fh, elt.infoFn)
				if err != nil {
					fmt.Printf("cannot append %s", elt.infoFn)
				}
				elt.info.Dpos, err = appendFile(tw, fh, elt.dataFn)
				if err != nil {
					fmt.Printf("cannot append %s", elt.dataFn)
				}
				if err = elt.cdbDump(iw); err != nil {
					fmt.Println("cannot dump cdb info")
					return err
				}
				delete(buf, key)
			}
		}
	}
	iw.Close()
	if len(buf) > 0 {
		fmt.Printf("remaining files: %+v", buf)
	}
	return err
}

func appendFile(tw *tar.Writer, tfh io.Seeker, fn string) (pos uint64, err error) {
	hdr, err := aostor.FileTarHeader(fn)
	if err != nil {
		return
	}
	sfh, err := os.Open(fn)
	if err != nil {
		return
	}
	defer sfh.Close()
	p, err := tfh.Seek(0, 1)
	if err != nil {
		return
	}
	pos = uint64(p)
	aostor.WriteTar(tw, hdr, sfh)
	tw.Flush()
	return
}

func (elt fElt) cdbDump(w io.Writer) error {
	data := elt.info.Bytes()
	_, err := fmt.Fprintf(w, "+%d,%d:%s->%s\n", len(elt.info.Key), len(data),
		elt.info.Key, data)
	return err
}

func main() {
	flag.Parse()
	tarfn, dirname := flag.Arg(0), flag.Arg(1)
	CreateTar(tarfn, dirname)
}
