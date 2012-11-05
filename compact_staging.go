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
	"errors"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"github.com/tgulacsi/go-locking"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type NotifyFunc func()

var stopIteration = errors.New("StopIteration")
var AlreadyLocked = errors.New("AlreadyLocked")

// compacts staging dir: moves info and data files to tar; calls CompactIndices
func Compact(realm string, onChange NotifyFunc) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	if locks, err := locking.FLockDirs(conf.IndexDir, conf.StagingDir); err != nil {
		logger.Error("cannot lock dir: ", err)
		return err
	} else {
		defer locks.Unlock()
	}

	n := DeDup(conf.StagingDir, conf.ContentHash, true)
	logger.Infof("DeDup: %d", n)

	var is, ds int64
	size := uint64(0)

	var hamster listDirFunc = func(elt fElt) error {
		is, ds = fileSize(elt.infoFn), int64(0)
		if elt.isSymlink {
			ds = int64(1)
		} else {
			ds = fileSize(elt.dataFn)
		}
		size += BS + inBs(is) + BS + inBs(ds)
		logger.Tracef("size=%d = %0.3fMb", size, float64(size)/1024.0/1024.0)
		if size >= conf.TarThreshold {
			return stopIteration
		}
		return nil
	}

	for {
		size = uint64(0)

		if err = listDirMap(conf.StagingDir, conf.ContentHash, hamster); err != nil {
			logger.Error("error compacting staging: ", err)
			return err
		}

		logger.Debugf("size=%d >= %d", size, conf.TarThreshold)
		if size < conf.TarThreshold {
			break
		}
		uuid, err := NewUUID()
		if err != nil {
			return err
		}
		tarfn := realm + "-" + strNow()[:15] + "-" + uuid.String() + ".tar"
		logger.Info("creating ", tarfn)
		if err = CreateTar(conf.TarDir+"/"+tarfn, conf.StagingDir, conf.TarThreshold, true); err != nil {
			return err
		}
		tarfn_a := conf.TarDir + "/" + tarfn
		if err = os.Symlink(tarfn_a+".cdb",
			conf.IndexDir+"/L00/"+tarfn+".cdb"); err != nil {
			return err
		}
		if err = cleanupStaging(conf.StagingDir, tarfn_a); err != nil {
			return err
		}
		if onChange != nil {
			onChange()
		}
	}
	logger.Info("staging compacted successfully")
	if err = CompactIndices(realm, 0, onChange, true); err != nil {
		logger.Error("error compacting indices: ", err)
		return err
	}
	logger.Info("indices compacted successfully")
	return nil
}

// func uuidKey(key string) []byte {
// 	uuid, err := UUIDFromString(key)
// 	if err != nil {
// 		logger.Errorf("cannot convert ", key, " to uuid: ", err)
// 		return []byte(key)
// 	}
// 	return uuid.Bytes()
// }

// Copies files from the given directory into a given tar file
func CreateTar(tarfn string, dirname string, sizeLimit uint64, alreadyLocked bool) error {
	if !alreadyLocked {
		if locks, err := locking.FLockDirs(dirname); err != nil {
			logger.Error("cannot lock dir: ", err)
			return err
		} else {
			defer locks.Unlock()
		}
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
	adder, closer, err := cdb.MakeFactory(cfh)
	if err != nil {
		logger.Criticalf("cannot create factory: %s", err)
		return err
	}
	links := make(map[string]uint64, 32)
	buf := make([]fElt, 0, 8)
	symlinks, err := harvestSymlinks(dirname)
	if err != nil {
		logger.Error("cannot read symlinks beforehand: ", err)
		return err
	}
	// logger.Info("symlinks=", symlinks)
	// if len(symlinks) == 0 {
	// 	os.Exit(5)
	// }

	var hamster listDirFunc = func(elt fElt) error {
		// logger.Printf("fn=%s -> key=%s ?%s", fn, key, isInfo)
		if elt.isSymlink {
			return nil
		}
		if !elt.info.Key.IsEmpty() {
			elt.info.Ipos = pos
			_, pos, err = appendFile(tw, fh, elt.infoFn)
			if err != nil {
				logger.Criticalf("cannot append %s", elt.infoFn)
				os.Exit(1)
			}
			elt.info.Dpos = pos
			links[elt.dataFn] = pos
			_, pos, err = appendFile(tw, fh, elt.dataFn)
			if err != nil {
				logger.Criticalf("cannot append %s", elt.dataFn)
				os.Exit(1)
			}

			for _, sym := range symlinks[elt.dataFn] {
				linkpos, ok := links[sym.dataFnOrig]
				logger.Debugf("adding %s (symlink of %s) linkpos? %s",
					sym.info.Key, elt.dataFn, ok)
				if !ok {
					buf = append(buf, sym)
					return nil
				}
				sym.info.Dpos = linkpos
				_, pos, err = appendLink(tw, fh, sym.dataFn)
				if err != nil {
					logger.Criticalf("cannot append %s", sym.dataFn)
					os.Exit(1)
				}
				adder(cdb.Element{sym.info.Key.Bytes(), sym.info.Bytes()})
			}
			delete(symlinks, elt.dataFn)
			// c <- cdb.Element{StrToBytes(elt.info.Key), elt.info.Bytes()}
			adder(cdb.Element{elt.info.Key.Bytes(), elt.info.Bytes()})
		}
		// logger.Tracef("buf=%s", buf)
		if pos > 0 && pos > sizeLimit {
			return stopIteration
		}
		return nil
	}
	err = listDirMap(dirname, "", hamster)
	if err != nil {
		logger.Criticalf("error listing %s: %s", dirname, err)
		return err
	}
	// close(c)
	// logger.Tracef("buf=%s", buf)

	for _, elt := range buf {
		linkpos, ok := links[elt.dataFnOrig]
		if !ok {
			logger.Warnf("cannot find linkpos for %s -> %s", elt.dataFn, elt.dataFnOrig)
			elt.info.Dpos = pos
			_, pos, err = appendFile(tw, fh, elt.dataFn)
		} else {
			elt.info.Dpos = linkpos
			_, pos, err = appendLink(tw, fh, elt.dataFn)
		}
		if err != nil {
			logger.Criticalf("cannot append %s", elt.dataFn)
			os.Exit(1)
		}
		// logger.Debugf("adding ",keyb," to ",)
		adder(cdb.Element{elt.info.Key.Bytes(), elt.info.Bytes()})
	}

	// iw.Close()
	if err != nil {
		fmt.Printf("error: %s", err)
	}
	err = closer()
	// err = <-d
	if err != nil {
		logger.Errorf("cdbMake error: %s", err)
	}
	return err
}

func harvestSymlinks(path string) (map[string][]fElt, error) {
	dh, err := os.Open(path)
	if err != nil {
		logger.Criticalf("cannot open dir %s: %s", path, err)
		os.Exit(1)
	}
	defer dh.Close()
	links := make(map[string][]fElt, 1024)
	var (
		linkpath, origin string
		elt              fElt
		ifh              io.ReadCloser
	)
	for {
		files, err := dh.Readdir(1024)
		if err != nil {
			if err != io.EOF {
				logger.Errorf("cannot list dir %s: %s", path, err)
			}
			break
		}
		for _, fi := range files {
			if fi.Mode()&os.ModeSymlink == 0 {
				continue
			}
			bn := fi.Name()
			linkpath = path + "/" + bn
			origin = FindLinkOrigin(linkpath)
			if !filepath.IsAbs(origin) {
				origin = path + "/" + origin
			}
			logger.Tracef("bn=%s origin=%s", bn, origin)
			if !(strings.HasSuffix(bn, SuffLink) && origin != linkpath &&
				fileExists(origin)) {
				continue
			}
			bn = bn[:len(bn)-1]
			elt = fElt{infoFn: path + "/" + bn + SuffInfo,
				dataFn: linkpath, isSymlink: true, dataFnOrig: origin}
			if ifh, err = os.Open(elt.infoFn); err != nil {
				logger.Errorf("cannot open info file %s: %s", elt.infoFn, err)
				return nil, err
			}
			elt.info, err = ReadInfo(ifh)
			ifh.Close()
			if err != nil {
				logger.Errorf("cannot read info from %s: %s", ifh, err)
				return nil, err
			}

			if arr, ok := links[elt.dataFnOrig]; !ok || arr == nil {
				links[elt.dataFnOrig] = make([]fElt, 0, 4)
			}
			links[elt.dataFnOrig] = append(links[elt.dataFnOrig], elt)
		}
	}
	return links, nil
}

// appends file to tar
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

// removes files already in tar
func cleanupStaging(path string, tarfn string) error {
	cfh, err := os.Open(tarfn + ".cdb")
	if err != nil {
		return err
	}
	defer cfh.Close()
	endings := []string{SuffData, SuffLink}
	return cdb.DumpMap(cfh, func(elt cdb.Element) error {
		uuid, err := UUIDFromBytes(elt.Key)
		if err != nil {
			logger.Errorf("cannot convert %s to uuid: %s", elt.Key, err)
			return err
		}
		base := path + "/" + uuid.String()
		// logger.Debugf("base %s exists? %s", base, fileExists(base+SuffInfo))
		if fileExists(base + SuffInfo) {
			for _, end := range endings {
				err = os.Remove(base + end)
				logger.Debugf("Remove(%s): %s", base+end, err)
				if err == nil {
					err = os.Remove(base + SuffInfo)
					if err != nil {
						logger.Errorf("error removing %s: %s", base+SuffInfo, err)
						return err
					}
					break
				}
			}
		}
		return nil
	})
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
				logger.Errorf("cannot list %s: %s", dirname, err)
				break
			}
		}
	}
	return n
}
