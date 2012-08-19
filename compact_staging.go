// Copyright 2012 Tamás Gulácsi, UNO-SOFT Computing Ltd.
//
// All rights reserved.
//
// This file is part of aostor.
//
// Aostor is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Aostor is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Aostor.  If not, see <http://www.gnu.org/licenses/>.

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

type NotifyFunc func()

// compacts staging dir: moves info and data files to tar
func CompactStaging(realm string, onChange NotifyFunc) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	n := DeDup(conf.StagingDir, conf.ContentHash)
	logger.Info("DeDup: %d", n)

	c := make(chan fElt, 1)
	go listDir(c, conf.StagingDir, "")
	size := uint64(0)
	for {
		elt, ok := <-c
		if !ok {
			break
		}
		size += inBs(fileSize(elt.infoFn))
		if elt.isSymlink {
			size += BS
		} else {
			size += inBs(fileSize(elt.dataFn))
		}
		logger.Debug("elt=%s => size=%d", elt, size)
		if size >= conf.TarThreshold {
			uuid, err := StrUUID()
			if err != nil {
				return err
			}
			tarfn := realm + "-" + strNow()[:15] + "-" + uuid + ".tar"
			if err = CreateTar(conf.TarDir+"/"+tarfn, conf.StagingDir,
					true, onChange); err != nil {
				return err
			}
			if err = os.Symlink(conf.TarDir+"/"+tarfn+".cdb",
					conf.IndexDir+"/L00/"+tarfn+".cdb"); err != nil {
				return err
			}
			if onChange != nil {
				onChange()
			}
			if err = CompactIndices(realm, 0); err != nil{
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
	hashes := make(map[string][]fElt, 16)
	primals := make(map[string][]string, 16)
	c := make(chan fElt, 1)
	go listDir(c, path, hash)
	for {
		elt, ok := <-c
		if !ok {
			break
		}
		logger.Trace("%s sl? %s lo=%s", elt.contentHash, elt.isSymlink, FindLinkOrigin(elt.dataFn))
		if elt.contentHash == "" {
			continue
		} else if elt.isSymlink {
			if prim, ok := primals[elt.contentHash]; !ok {
				primals[elt.contentHash] = []string{elt.dataFnOrig}
			} else {
				primals[elt.contentHash] = append(prim, elt.dataFnOrig)
			}
		} else if other, ok := hashes[elt.contentHash]; ok {
			hashes[elt.contentHash] = append(other, elt)
		} else {
			hashes[elt.contentHash] = []fElt{elt}
		}
	}
	//logger.Printf("hashes=%s", hashes)
	//logger.Printf("primals=%s", primals)

	// TODO: primals for first in hashes
	for _, elts := range hashes {
		other := fElt{}
		for _, elt := range elts {
			if prim, ok := primals[elt.contentHash]; ok {
				contains := false
				for _, df := range prim {
					if df == elt.dataFn {
						contains = true
						break
					}
				}
				if contains {
					continue
				}
			}
			if other.dataFn == "" {
				other = elt
				continue
			}
			if err := os.Remove(elt.dataFn); err != nil {
				logger.Warn("cannot remove %s: %s", elt.dataFn, err)
			} else {
				p := strings.LastIndex(elt.dataFn, "#")
				if err := os.Symlink(other.dataFn, elt.dataFn[:p]+SuffLink); err != nil {
					logger.Warn("cannot create symlink %s for %s: %s", elt.dataFn, other.dataFn, err)
				} else {
					n++
				}
			}
		}
	}
	return n
}

type fElt struct {
	info        Info
	infoFn      string
	dataFn      string
	contentHash string
	isSymlink   bool
	dataFnOrig  string
}

//Moves files from the given directory into a given tar file
func CreateTar(tarfn string, dirname string, move bool, onChange NotifyFunc) error {
	var tbd []string
	if move {
		tbd = make([]string, 0)
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
	links := make(map[string]uint64, 32)
	buf := make([]fElt, 0)

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
				logger.Critical("cannot append %s", elt.infoFn)
				os.Exit(1)
			}
			if elt.isSymlink {
				linkpos, ok := links[elt.dataFnOrig]
				if !ok {
					buf = append(buf, elt)
					continue
				}
				elt.info.Dpos = linkpos
				_, pos, err = appendLink(tw, fh, elt.dataFn)
				if err != nil {
					logger.Critical("cannot append %s", elt.dataFn)
					os.Exit(1)
				}
			} else {
				elt.info.Dpos = pos
				links[elt.dataFn] = pos
				_, pos, err = appendFile(tw, fh, elt.dataFn)
				if err != nil {
					logger.Critical("cannot append %s", elt.dataFn)
					os.Exit(1)
				}
			}
			c <- cdb.Element{StrToBytes(elt.info.Key), elt.info.Bytes()}
			if move {
				tbd = append(tbd, elt.infoFn, elt.dataFn)
			}
		}
	}
	close(c)

	for _, elt := range buf {
		linkpos, ok := links[elt.dataFnOrig]
		if !ok {
			logger.Warn("cannot find linkpos for %s -> %s", elt.dataFn, elt.dataFnOrig)
			elt.info.Dpos = pos
			_, pos, err = appendFile(tw, fh, elt.dataFn)
		} else {
			elt.info.Dpos = linkpos
			_, pos, err = appendLink(tw, fh, elt.dataFn)
		}
		if err != nil {
			logger.Critical("cannot append %s", elt.dataFn)
			os.Exit(1)
		} else if move {
			tbd = append(tbd, elt.infoFn, elt.dataFn)
		}
	}

	// iw.Close()
	if err != nil {
		fmt.Printf("error: %s", err)
	}
	err = <-d
	cfh.Close()
	if err != nil {
		logger.Error("cdbMake error: %s", err)
	}
	if move && err == nil && len(tbd) > 0 {
		if onChange != nil {
			onChange()
		}
		for _, fn := range tbd {
			os.Remove(fn)
		}
		if onChange != nil {
			onChange()
		}
	}
	return err
}

func listDir(c chan<- fElt, path string, hash string) {
	defer close(c)
	possibleEndings := []string{"bz2", "gz"}
	dh, err := os.Open(path)
	if err != nil {
		logger.Critical("cannot open dir %s: %s", path, err)
		os.Exit(1)
	}
	var (
		info, emptyInfo Info
		elt, emptyElt   fElt
	)
	emptyElt.isSymlink = false
	buf := make([]fElt, 0)
	for {
		keyfiles, err := dh.Readdir(1024)
		if err != nil {
			if err != io.EOF {
				logger.Error("cannot list dir %s: %s", path, err)
			}
			break
		}
		for _, fi := range keyfiles {
			bn := fi.Name()
			if !strings.HasSuffix(bn, SuffInfo) || !fileExists(path+"/"+bn) {
				continue
			}
			info, elt = emptyInfo, emptyElt
			elt.infoFn = path + "/" + bn
			// bn := BaseName(fn)
			info.Key = bn[:len(bn)-1]
			if ifh, err := os.Open(elt.infoFn); err == nil {
				info, err = ReadInfo(ifh)
				ifh.Close()
				if err != nil {
					logger.Error("cannot read info from %s: %s", elt.infoFn, err)
					continue
				}
			} else {
				logger.Error("cannot read info from %s: %s", elt.infoFn, err)
				continue
			}

			pref := path + "/" + info.Key
			if fileExists(pref + SuffLink) {
				elt.dataFn = pref + SuffLink
				elt.isSymlink = true
			} else {
				pref += SuffData
				for _, end := range possibleEndings {
					// logger.Printf("checking %s: %s", pref + end, fileExists(pref+end))
					if fileExists(pref + end) {
						elt.dataFn = pref + end
						break
					}
				}
				if elt.dataFn == "" {
					// logger.Printf("cannot find data file for %s", fn)
					continue
				}
			}
			elt.info = info
			if hash != "" {
				elt.contentHash = info.Get(InfoPref + "Content-" + hash)
			}
			if elt.isSymlink {
				elt.dataFnOrig = FindLinkOrigin(elt.dataFn)
				buf = append(buf, elt)
			} else {
				c <- elt
			}
		}
	}
	for _, elt := range buf {
		c <- elt
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

// FIXME
func appendLink(tw *tar.Writer, tfh io.Seeker, fn string) (pos1 uint64, pos2 uint64, err error) {
	if !fileIsSymlink(fn) {
		return appendFile(tw, tfh, fn)
	}
	hdr, err := FileTarHeader(fn)
	hdr.Size = 0
	hdr.Typeflag = tar.TypeSymlink
	hdr.Linkname = BaseName(FindLinkOrigin(fn))
	// logger.Printf("fn=%s hdr=%+v tm=%s", fn, hdr, hdr.Typeflag)
	if err != nil {
		return
	}
	p, err := tfh.Seek(0, 1)
	if err != nil {
		return
	}
	pos1 = uint64(p)
	WriteTar(tw, hdr, nil)
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

func fileIsSymlink(fn string) bool {
	fi, err := os.Lstat(fn)
	if err == nil {
		return fi.Mode()&os.ModeSymlink > 0
	}
	return false
}

func dirCount(dirname string) uint64 {
	n := uint64(0)
	if dh, err := os.Open(dirname); err == nil {
		defer dh.Close()
		for {
			files, err := dh.Readdir(1024)
			if err == nil {
				n += uint64(len(files))
			} else if err == io.EOF {
				break
			} else {
				logger.Error("cannot list %s: %s", dirname, err)
				break
			}
		}
	}
	return n
}
