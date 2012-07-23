package main

import (
	"archive/tar"
	"flag"
	"fmt"
	// "github.com/jbarham/go-cdb"
	// "cdb"
	"github.com/tgulacsi/go-cdb"
	//"path"
	"io"
	"log"
	"os"
	"strings"
	"unosoft.hu/aostor/tarhelper"
)

var logger = log.New(os.Stderr, "tarhelper ", log.LstdFlags|log.Lshortfile)

type fElt struct {
	info   aostor.Info
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
	tw, fh, pos, err := aostor.OpenForAppend(tarfn)
	//fh, err := os.OpenFile(tarfn, os.O_APPEND|os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	defer fh.Close()
	//tw := tar.NewWriter(fh)
	defer tw.Close()

	cfh, err := os.OpenFile(tarfn+".cdb", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer cfh.Close()
	/*
		ir, iw := io.Pipe()
		c := make(chan error)
		go cdbMake(c, cfh, ir)
	*/
	c := make(chan cdb.Element, 1)
	d := make(chan error, 0)
	go cdb.MakeFromChan(cfh, c, d)

	var (
		buf         map[string]fElt = make(map[string]fElt, 32)
		key, bn, fn string
		info        aostor.Info
		isInfo      bool
	)
	for _, file := range list {
		bn = file.Name()
		fn = dirname + "/" + bn
		switch {
		case strings.HasSuffix(bn, aostor.SuffInfo):
			key, isInfo = bn[:len(bn)-1], true
			if ifh, err := os.Open(dirname + "/" + bn); err == nil {
				info = aostor.ReadInfo(ifh)
				ifh.Close()
			} else {
				logger.Printf("cannot read info from %s: %s", fn, err)
			}
		case strings.Contains(bn, aostor.SuffLink):
			key, isInfo = strings.Split(bn, aostor.SuffLink)[0], false
		case strings.Contains(bn, aostor.SuffData):
			key, isInfo = strings.Split(bn, aostor.SuffData)[0], false
		default:
			key, isInfo = "", true
		}
		// logger.Printf("fn=%s -> key=%s ?%s", fn, key, isInfo)
		if key != "" {
			elt, ok := buf[key]
			if isInfo {
				elt.infoFn = fn
				elt.info = info
			} else {
				elt.dataFn = fn
			}
			if ok && elt.infoFn != "" && elt.dataFn != "" { //full
				elt.info.Key = key
				elt.info.Ipos = pos
				_, pos, err = appendFile(tw, fh, elt.infoFn)
				if err != nil {
					logger.Panicf("cannot append %s", elt.infoFn)
				}
				elt.info.Dpos = pos
				_, pos, err = appendFile(tw, fh, elt.dataFn)
				if err != nil {
					logger.Panicf("cannot append %s", elt.dataFn)
				}
				/*
					if err = elt.cdbDump(iw); err != nil {
						logger.Panicf("cannot dump cdb info: %s", err)
						return err
					}
				*/
				c <- cdb.Element{aostor.StrToBytes(key), elt.info.Bytes()}
				delete(buf, key)
			} else {
				buf[key] = elt
			}
		}
	}
	c <- cdb.Element{}
	// iw.Close()
	if err != nil {
		fmt.Printf("error: %s", err)
	}
	err = <-d
	cfh.Close()
	if err != nil {
		logger.Printf("cdbMake error: %s", err)
	}
	if len(buf) > 0 {
		logger.Printf("remaining files: %+v", buf)
	}
	return err
}

func appendFile(tw *tar.Writer, tfh io.Seeker, fn string) (pos1 uint64, pos2 uint64, err error) {
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
	pos1 = uint64(p)
	aostor.WriteTar(tw, hdr, sfh)
	tw.Flush()
	p, err = tfh.Seek(0, 1)
	if err != nil {
		return
	}
	pos2 = uint64(p)
	return
}

func (elt fElt) cdbDump(w io.Writer) error {
	data := elt.info.Bytes()
	_, err := fmt.Fprintf(w, "+%d,%d:%s->%s\n", len(elt.info.Key), len(data),
		elt.info.Key, data)
	return err
}

/*
func cdbMake(c chan <-error, w io.WriteSeeker, r io.Reader) {
	c <- cdb.Make(w, r)
}
*/

func main() {
	flag.Parse()
	tarfn, dirname := flag.Arg(0), flag.Arg(1)
	if err := CreateTar(tarfn, dirname); err != nil {
		fmt.Printf("ERROR: %s", err)
	} else {
		fmt.Println("OK")
	}
}
