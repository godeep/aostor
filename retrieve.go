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
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var NotFound = errors.New("Not Found")

var (
	cdbFiles  = map[string][2][]string{}
	tarFiles  = map[string](map[string]string){}
	cacheLock = sync.RWMutex{}
)

//returns the associated info and data of a given uuid in a given realm
//  1. checks staging area
//  2. checks level zero (symlinked cdbs in ndx/L00)
//  3. checks higher level (older, too) cdbs in ndx/L01, ndx/L02...
//
//The difference between level zero and higher is the following: at level zero,
//there are the tar files' cdbs (symlinked), and these cdbs contains the info
//(with the position information), ready to serve.
//At higher levels, the cdbs contains only "/%d" signs (which cdb,
//only a number) and that sign is which zero-level cdb. So at this level an
//additional lookup is required.
func Get(realm string, uuid UUID) (info Info, reader io.Reader, err error) {
	conf, err := ReadConf("", realm)
	if err != nil {
		logger.Error("cannot read config: %s", err)
		return
	}
	for {
		// L00
		info, reader, err = findAtStaging(uuid, conf.StagingDir)
		if err == nil {
			//logger.Printf("found at staging: %s", info)
			return
		} else if !os.IsNotExist(err) {
			logger.Error("error searching at staging: %s", err)
		}

		fillCdbCache(realm, conf.IndexDir, false)
		info, reader, err = findAtLevelZero(realm, uuid)
		if err == nil {
			//logger.Printf("found at level zero: %s", info)
			return
		} else if err != NotFound && os.IsNotExist(err) {
			FillCaches(true)
			continue
		}
		fillTarCache(realm, conf.TarDir, false)
		info, reader, err = findAtLevelHigher(realm, uuid, conf.IndexDir)
		if err == nil {
			return
		}
		FillCaches(true)
		logger.Warn("LOOP AGAIN as searching for %s@%s", uuid, realm)
	}
	return
}

//fills caches (reads tar files and cdb files, caches path)
func FillCaches(force bool) error {
	config, err := ReadConf("", "")
	logger.Info("FillCaches on %s", config)
	if err != nil {
		return err
	}
	for _, realm := range config.Realms {
		conf, err := ReadConf("", realm)
		if err != nil {
			return err
		}
		fillCdbCache(realm, conf.IndexDir, force)
		fillTarCache(realm, conf.TarDir, force)
	}
	return nil
}

//fills the (cached) cdb files list. Rereads if force is true
func fillCdbCache(realm string, indexdir string, force bool) {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if !force {
		cf, ok := cdbFiles[realm]
		if ok && (len(cf[0]) > 0 || len(cf[1]) > 0) {
			return
		}
	}

	pat := indexdir + "/L00/*.cdb"
	files, err := filepath.Glob(pat)
	if err != nil {
		logger.Error("cannot list %s: %s", pat, err)
	}
	cf := [2][]string{files, make([]string, 0, 100)}
	logger.Info("fillCdbCache(%s): %d", realm, len(cdbFiles[realm][0]))

	for level := 1; level < 1000; level++ {
		dn := indexdir + fmt.Sprintf("/L%02d", level)
		// logger.Printf("dn: %s", dn)
		if !fileExists(dn) {
			// logger.Printf("%s not exists", dn)
			break
		}
		pat = dn + "/*.cdb"
		files, err = filepath.Glob(pat)
		if err != nil {
			logger.Error("cannot list %s: %s", pat, err)
		}
		// logger.Printf("%s => %+v", dn, files)
		cf[1] = append(cf[1], files...)
	}
	cdbFiles[realm] = cf
	// logger.Printf("fillCdbCache(%s): %d", realm, len(cdbFiles[realm][1]))
	// logger.Printf("%+v", cf)
	// logger.Printf("%+v", cdbFiles[realm])
	// logger.Printf("fillCdbCache(%s, %s, %s): %+v",
	// 	realm, indexdir, force, cdbFiles)
}

func fillTarCache(realm string, tardir string, force bool) {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if !force {
		tf, ok := tarFiles[realm]
		if ok && len(tf) > 0 {
			return
		}
	}

	tf := make(map[string]string, 1000)
	filepath.Walk(tardir, func(fn string, info os.FileInfo, err error) error {
		if err != nil {
			if info.IsDir() {
				return filepath.SkipDir
			} else {
				return nil
			}
		} else {
			if strings.HasSuffix(info.Name(), ".tar") {
				bn := info.Name()
				tf[bn] = fn
				uuid := bn[:len(bn)-4] //213-uuid.tar
				p := strings.LastIndex(uuid, "-")
				if p >= 0 {
					uuid = uuid[p+1:]
				} else if len(uuid) > 32 {
					uuid = uuid[len(uuid)-32:]
				}
				tf[uuid] = fn
			}
		}
		return nil
	})
	tarFiles[realm] = tf
	logger.Info("fillTarCache(%s): %d", realm, len(tarFiles[realm]))
	// logger.Printf("fillTarCache(%s, %s, %s): %+v",
	// 	realm, tardir, force, tarFiles)
}

func findAtLevelHigher(realm string, uuid UUID, tardir string) (info Info, reader io.Reader, err error) {
	var tarfn_b string
	logger.Debug("findAtLevelHigher(%s, %s) files=%+v", realm, uuid, cdbFiles[realm][1])
	cacheLock.RLock()
	defer cacheLock.RUnlock()
	logger.Trace("%+v", cdbFiles)
	for _, cdb_fn := range cdbFiles[realm][1] {
		db, err := cdb.Open(cdb_fn)
		if err != nil {
			return info, nil, err
		}
		indx, err := db.Data(uuid.Bytes())
		logger.Debug("findAtLevelHigher(%s, %s) @ %s ? (%s, %s)",
			realm, uuid, cdb_fn, indx, err)
		switch err {
		case nil:
			data, err := db.Data(indx)
			db.Close()
			if err != nil {
				logger.Error("cannot get %s from %s: %s", indx, cdb_fn, err)
				return info, nil, err
			}
			tarfn_b = BytesToStr(data)
			break
		case io.EOF:
			db.Close()
			continue
		default:
			db.Close()
			return info, nil, err
		}
		db.Close()
		logger.Debug("searching %s at %s: %s %s", uuid, tardir, tarfn_b, err)
	}
	if err != nil {
		if err == io.EOF {
			err = NotFound
		}
		return
	}
	if tarfn_b != "" {
		tarfn, ok := tarFiles[realm][tarfn_b]
		logger.Trace("tarfn_b=%s => %s (%s)", tarfn_b, tarfn, ok)
		if !ok {
			logger.Error("cannot find tarfile for %s!", tarfn)
			err = NotFound
			return
		}
		info, reader, err = GetFromCdb(uuid, tarfn+".cdb")
		logger.Debug("found %s/%s in %s(%s): %s",
			realm, uuid, tarfn, tarfn_b, info)
	} else {
		err = NotFound
	}
	return
}

func GetFromCdb(uuid UUID, cdb_fn string) (info Info, reader io.Reader, err error) {
	db, err := cdb.Open(cdb_fn)
	defer db.Close()
	if err != nil {
		logger.Error("cannot open %s: %s", cdb_fn, err)
		return
	}
	data, err := db.Data(uuid.Bytes())
	if err != nil {
		if err == io.EOF || err == NotFound {
			logger.Info("cannot find %s in %s", uuid, cdb_fn)
			err = NotFound
		} else {
			logger.Error("cannot find %s in %s: %s", uuid, cdb_fn, err)
		}
		return
	}
	if len(data) == 0 {
		logger.Warn("got zero length data from %s for %s", cdb_fn, uuid)
		err = NotFound
		return
	}
	info, err = ReadInfo(bytes.NewReader(data))
	if err != nil {
		logger.Error("cannot read info from %s: %s", data, err)
		return
	}
	if info.Dpos == 0 {
		logger.Warn("got zero Dpos from %so for %s", cdb_fn, uuid)
		err = NotFound
		return
	}
	ocdb := FindLinkOrigin(cdb_fn)
	//logger.Printf("cdb_fn=%s == %s", cdb_fn, ocdb)
	tarfn := ocdb[:len(ocdb)-4]
	reader, err = ReadItem(tarfn, int64(info.Dpos))
	if err != nil {
		logger.Error("GetFromCdb(%s, %s) -> ReadItem(%s, %d) error: %s",
			uuid, cdb_fn, tarfn, info.Dpos)
	} else {
		logger.Debug("GetFromCdb found %s in %s: tarfn=%s, info=%s",
			uuid, cdb_fn, tarfn, info)
	}
	return
}

func fileExists(fn string) bool {
	fh, err := os.Open(fn)
	if err == nil {
		fh.Close()
		return true
	} else {
		return !os.IsNotExist(err)
	}
	return false
}

func findAtLevelZero(realm string, uuid UUID) (info Info, reader io.Reader, err error) {
	logger.Debug("L00 files at %s: %s", realm, cdbFiles[realm][0])
	cacheLock.RLock()
	defer cacheLock.RUnlock()
	for _, cdb_fn := range cdbFiles[realm][0] {
		info, reader, err = GetFromCdb(uuid, cdb_fn)
		switch err {
		case nil:
			logger.Debug("L00 found %s in %s: %s", uuid, cdb_fn, info)
			return
		case io.EOF:
			continue
		default:
			logger.Error("L00 error in GetFromCdb(%s, %s): %s", uuid, cdb_fn, err)
			return info, nil, err
		}
	}

	logger.Debug("findAtLevelZero(%s, %s): %s", realm, uuid, info)
	return info, nil, NotFound
}

func findAtStaging(uuid UUID, path string) (info Info, reader io.Reader, err error) {
	ifh, err := os.Open(path + "/" + uuid.String() + SuffInfo)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Error("findAtStaging(%s) other error: %s", uuid, err)
			return
		}
		return
	}
	logger.Debug("L-1 found %s at %s as %s", uuid, path, ifh)
	info, err = ReadInfo(ifh)
	ifh.Close()
	if err != nil {
		logger.Error("cannot read info file %s: %s", ifh, err)
		return
	}
	// var suffixes = []string{SuffData + "bz2", SuffData + "gz", SuffLink, SuffData}
	var suffixes = []string{SuffData, SuffLink}
	var fn string
	ct := info.Get("Content-Type")
	for _, suffix := range suffixes {
		fn = path + "/" + uuid.String() + suffix
		if fileExists(fn) {
			fh, err := os.Open(fn)
			if err != nil {
				logger.Error("cannot open %s: %s", fn, err)
				return Info{}, nil, err
			}
			if suffix == SuffLink {
				fn = FindLinkOrigin(fn)
				// suffix = fn[strings.LastIndex(fn, "#"):]
				ifn := fn[:len(fn)-1] + "!"
				ifh_o, err := os.Open(ifn)
				if err != nil {
					logger.Error("fintAtStaging(%s) symlink %s error: %s", uuid, ifn, err)
					return info, nil, err
				}
				info_o, err := ReadInfo(ifh_o)
				ifh_o.Close()
				if err != nil {
					logger.Error("cannot read symlink info %s: %s", ifh_o, err)
					return info, nil, err
				}
				ct = info_o.Get("Content-Type")
			}
			switch ct {
			case "bz2":
				reader, err = bzip2.NewReader(fh), nil
			case "gzip":
				reader, err = gzip.NewReader(fh)
			default:
				reader, err = fh, nil
			}
			return info, reader, err
		}
	}
	return Info{}, nil, os.ErrNotExist
}

var suffopeners = []suffOpener{
	suffOpener{SuffData + "bz2",
		func(r io.Reader) (io.Reader, error) {
			return bzip2.NewReader(r), nil
		}},
	suffOpener{SuffData + "gz",
		func(r io.Reader) (io.Reader, error) {
			return gzip.NewReader(r)
		}},
	suffOpener{SuffLink, nil},
	suffOpener{SuffData, nil}}

type suffOpener struct {
	suffix string
	open   func(io.Reader) (io.Reader, error)
}

func FindLinkOrigin(fn string) string {
	for {
		fi, err := os.Lstat(fn)
		if err != nil {
			break
		}
		//logger.Printf("%s mode=%s symlink? %d", fn, fi.Mode(), fi.Mode() & os.ModeSymlink)
		if fi.Mode()&os.ModeSymlink == 0 {
			break
		}
		fn, err = os.Readlink(fn)
		if err != nil {
			logger.Error("error following link of %s: %s", fn, err)
			break
		}
	}
	return fn
}
