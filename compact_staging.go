package aostor

import (
	"archive/tar"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func CompactStaging(realm string) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	files, err := filepath.Glob(conf.StagingDir + "/*!")
	if err != nil {
		return err
	}
	size := uint64(0)
	for _, fn := range(files) {
		fs := fileSize(fn)
		if fs > 0 {
			size += uint64(fs)
			if size >= conf.TarThreshold {
				uuid, err := StrUUID()
				if err != nil {
					return err
				}
				CreateTar(conf.TarDir + "/" + realm + "-" + strNow() + "-" + uuid + ".tar",
					conf.StagingDir, true)
			}
		}
	}
	return nil
}

type fElt struct {
	info   Info
	infoFn string
	dataFn string
}

//Moves files from the given directory into a given tar file
func CreateTar(tarfn string, dirname string, move bool) error {
	var tbd []string
	if move {
		tbd = make([]string, 1024)
	}
	dh, err := os.Open(dirname)
	if err != nil {
		return err
	}
	defer dh.Close()
	list, err := dh.Readdir(-1)
	if err != nil {
		return err
	}
	tw, fh, pos, err := OpenForAppend(tarfn)
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
		info        Info
		isInfo      bool
	)
	for _, file := range list {
		bn = file.Name()
		fn = dirname + "/" + bn
		switch {
		case strings.HasSuffix(bn, SuffInfo):
			key, isInfo = bn[:len(bn)-1], true
			if ifh, err := os.Open(dirname + "/" + bn); err == nil {
				info, err = ReadInfo(ifh)
				if err != nil {
					logger.Printf("cannot read info from %s: %s", fn, err)
					continue
				}
				ifh.Close()
			} else {
				logger.Printf("cannot read info from %s: %s", fn, err)
				continue
			}
		case strings.Contains(bn, SuffLink):
			key, isInfo = strings.Split(bn, SuffLink)[0], false
		case strings.Contains(bn, SuffData):
			key, isInfo = strings.Split(bn, SuffData)[0], false
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
				c <- cdb.Element{StrToBytes(key), elt.info.Bytes()}
				delete(buf, key)
				if move {
					tbd = append(tbd, elt.infoFn, elt.dataFn)
				}
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
	if move && err == nil {
		for _, fn := range(tbd) {
			os.Remove(fn)
		}
	}
	return err
}

func appendFile(tw *tar.Writer, tfh io.Seeker, fn string) (pos1 uint64, pos2 uint64, err error) {
	hdr, err := FileTarHeader(fn)
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
	WriteTar(tw, hdr, sfh)
	tw.Flush()
	p, err = tfh.Seek(0, 1)
	if err != nil {
		return
	}
	pos2 = uint64(p)
	return
}
