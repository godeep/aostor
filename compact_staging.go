package aostor

import (
	"archive/tar"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	// "path/filepath"
	"strings"
)

// compacts staging dir: moves info and data files to tar
func CompactStaging(realm string) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	DeDup(conf.StagingDir, conf.ContentHash)
	c := make(chan fElt, 1)
	go listDir(c, conf.StagingDir, "")
	size := uint64(0)
	for {
		elt, ok := <-c
		if !ok {
			break
		}
		size += inBs(fileSize(elt.infoFn))
		size += inBs(fileSize(elt.dataFn))
		logger.Printf("elt=%s => size=%d", elt, size)
		if size >= conf.TarThreshold {
			uuid, err := StrUUID()
			if err != nil {
				return err
			}
			tarfn := realm + "-" + strNow()[:15] + "-" + uuid + ".tar"
			if err = CreateTar(conf.TarDir+"/"+tarfn, conf.StagingDir, true); err != nil {
				return err
			}
			if err = os.Symlink(conf.TarDir+"/"+tarfn+".cdb", conf.IndexDir+"/L00/"+tarfn+".cdb"); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

// deduplication: replace data with a symlink to a previous data with the same contant-hash-...
func DeDup(path string, hash string) int {
	n := 0
	hashes := make(map[string]fElt, 16)
	c := make(chan fElt, 1)
	go listDir(c, path, hash)
	for {
		elt, ok := <-c
		if !ok {
			break
		}
		if other, ok := hashes[elt.contentHash]; ok {
			if err := os.Remove(elt.dataFn); err != nil {
				logger.Printf("cannot remove %s: %s", elt.dataFn, err)
			} else {
				if err := os.Symlink(other.dataFn, elt.dataFn); err != nil {
					logger.Printf("cannot create symlink %s for %s: %s", elt.dataFn, other.dataFn, err)
				} else {
					n++
				}
			}
		} else {
			hashes[elt.contentHash] = elt
		}
	}
	return n
}

type fElt struct {
	info        Info
	infoFn      string
	dataFn      string
	contentHash string
}

//Moves files from the given directory into a given tar file
func CreateTar(tarfn string, dirname string, move bool) error {
	var tbd []string
	if move {
		tbd = make([]string, 1024)
	}
	listc := make(chan fElt, 1)
	go listDir(listc, dirname, "")
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
	d := make(chan error, 1)
	go cdb.MakeFromChan(cfh, c, d)

	for {
		elt, ok := <-listc
		if !ok {
			break
		}
		// logger.Printf("fn=%s -> key=%s ?%s", fn, key, isInfo)
		if elt.info.Key != "" {
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
			c <- cdb.Element{StrToBytes(elt.info.Key), elt.info.Bytes()}
			if move {
				tbd = append(tbd, elt.infoFn, elt.dataFn)
			}
		}
	}
	close(c)
	// iw.Close()
	if err != nil {
		fmt.Printf("error: %s", err)
	}
	err = <-d
	cfh.Close()
	if err != nil {
		logger.Printf("cdbMake error: %s", err)
	}
	if move && err == nil {
		for _, fn := range tbd {
			os.Remove(fn)
		}
	}
	return err
}

func listDir(c chan<- fElt, path string, hash string) {
	defer close(c)
	dh, err := os.Open(path)
	if err != nil {
		logger.Printf("cannot open dir %s: %s", path, err)
	}
	defer dh.Close()
	list, err := dh.Readdir(-1)
	if err != nil {
		logger.Panicf("cannot list dir %s: %s", path, err)
	}
	var (
		key    string
		isInfo bool
		info   Info
	)
	buf := make(map[string]fElt, 32)
	for _, file := range list {
		info = Info{}
		bn := file.Name()
		fn := path + "/" + bn
		switch {
		case strings.HasSuffix(bn, SuffInfo):
			key, isInfo = bn[:len(bn)-1], true
			if ifh, err := os.Open(path + "/" + bn); err == nil {
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
				if hash != "" {
					elt.contentHash = info.Get(InfoPref + "Content-" + hash)
				}
			} else {
				elt.dataFn = fn
			}
			if ok && elt.infoFn != "" && elt.dataFn != "" { //full
				elt.info.Key = key
				c <- elt
				delete(buf, key)
			} else if !ok {
				buf[key] = elt
			}
		}
	}
	if len(buf) > 0 {
		logger.Printf("remaining files: %s", buf)
	}
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

func inBs(size int64) uint64 {
	if size <= 0 {
		return uint64(0)
	}
	return (uint64(size)/512 + 1) * 512
}
