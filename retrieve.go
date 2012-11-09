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
	"time"
)

var (
	NotFound      = errors.New("Not Found")
	StopIteration = errors.New("StopIteration")
)

var (
	cdbFiles  = make(map[string][][]string, 100)
	tarFiles  = make(map[string](map[string]string), 100)
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
		logger.Errorf("cannot read config: %s", err)
		return
	}
	tries := 0
	for tries < 3 {
		// L00
		if info, reader, err = findAtStaging(uuid, conf.StagingDir); err == nil {
			//logger.Printf("found at staging: %s", info)
			return
		} else if !os.IsNotExist(err) {
			logger.Error("error searching at staging: ", err)
			return
		}

		if err = fillCdbCache(realm, conf.IndexDir, false); err != nil {
			return
		}
		logger.Debugf("findAtLevelZero(%s, %s)", realm, uuid)
		if info, reader, err = findAtLevelZero(realm, uuid); err == nil {
			//logger.Printf("found at level zero: %s", info)
			return
		}
		if err == NotFound {
			if err = fillTarCache(realm, conf.TarDir, false); err != nil {
				return
			}
			logger.Debugf("findAtLevelHigher(%s, %s)", realm, uuid)
			if info, reader, err = findAtLevelHigher(realm, uuid); err == nil {
				return
			}
			// logger.Debug("ERR: ", err, " ? ", os.IsNotExist(err))
			if !os.IsNotExist(err) {
				if err == NotFound && tries < 2 {
					tries++
				} else {
					return
				}
			}
		}
		// force cache reload
		logger.Warn("force cache reload")
		if err = FillCaches(true); err != nil {
			logger.Error("error with cache reload: ", err)
			return
		}
		logger.Warnf("LOOP AGAIN as searching for %s@%s", uuid, realm)
		time.Sleep(time.Second)
	}
	return
}

//fills caches (reads tar files and cdb files, caches path)
func FillCaches(force bool) error {
	config, err := ReadConf("", "")
	logger.Infof("FillCaches on %s", config)
	if err != nil {
		return err
	}
	for _, realm := range config.Realms {
		logger.Debug("BEGIN ReadConf")
		conf, err := ReadConf("", realm)
		logger.Debug("END ReadConf")
		if err != nil {
			return err
		}
		if err = fillCdbCache(realm, conf.IndexDir, force); err != nil {
			return err
		}
		if err = fillTarCache(realm, conf.TarDir, force); err != nil {
			return err
		}
	}
	return nil
}

//fills the (cached) cdb files list. Rereads if force is true
func fillCdbCache(realm string, indexdir string, force bool) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()

	if !force && cdbFiles != nil && len(cdbFiles) > 0 {
		cf, ok := cdbFiles[realm]
		if ok && cf != nil && len(cf) > 0 {
			return nil
		}
	}

	cf := make([][]string, 1, 10)
	err := walkCdbFiles(realm, indexdir, func(level int, fn string) error {
		for i := len(cf); i <= level; i++ {
			cf = append(cf, make([]string, 0, 10))
		}
		logger.Tracef("adding %s to cf[%d]", fn, level)
		cf[level] = append(cf[level], fn)
		return nil
	})
	logger.Debug("cf=", len(cf))
	if err != nil {
		logger.Error("Error in fillCdbCache: %s", err)
		return err
	}
	cdbFiles[realm] = cf
	return nil
}

func walkCdbFiles(realm, indexdir string, todo func(level int, fn string) error) error {
	pat := indexdir + "/L00/*.cdb"
	files, err := filepath.Glob(pat)
	if err != nil {
		logger.Error("cannot list %s: %s", pat, err)
		return err
	}
	for _, fn := range files {
		if err = todo(0, fn); err != nil {
			return err
		}
	}

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
			return err
		}
		for _, fn := range files {
			if err = todo(level, fn); err != nil {
				if err == StopIteration {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func fillTarCache(realm string, tardir string, force bool) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if !force && tarFiles != nil && len(tarFiles) > 0 {
		tf, ok := tarFiles[realm]
		if ok && tf != nil && len(tf) > 0 {
			return nil
		}
	}

	tf := make(map[string]string, 1000)
	err := walkTarFiles(realm, tardir, func(uuid, fn string) error {
		tf[filepath.Base(fn)] = fn
		tf[uuid] = fn
		return nil
	})
	if err != nil {
		logger.Error("error with fillTarCache: ", err)
	}
	logger.Infof("fillTarCache(%s): %d", realm, len(tf))
	tarFiles[realm] = tf
	return nil
}

func walkTarFiles(realm, tardir string, todo func(uuid, fn string) error) error {
	err := filepath.Walk(tardir,
		func(fn string, info os.FileInfo, err error) error {
			if err != nil {
				if info.IsDir() {
					return filepath.SkipDir
				} else {
					return nil
				}
			} else {
				if strings.HasSuffix(info.Name(), ".tar") {
					bn := info.Name()
					uuid := bn[:len(bn)-4] //213-uuid.tar
					p := strings.LastIndex(uuid, "-")
					if p >= 0 {
						uuid = uuid[p+1:]
					} else if len(uuid) > 32 {
						uuid = uuid[len(uuid)-32:]
					}

					return todo(uuid, fn)
				}
			}
			return nil
		})
	if err == StopIteration {
		err = nil
	}
	return err
}

func findAtLevelHigher(realm string, uuid UUID) (info Info, reader io.Reader, err error) {
	var tarfn_b string
	cacheLock.RLock()
	defer cacheLock.RUnlock()
	if cdbFiles[realm] == nil || len(cdbFiles[realm]) < 2 {
		logger.Error("empty cdbFiles? ", cdbFiles[realm])
		err = NotFound
		return
	}
	logger.Debugf("findAtLevelHigher(%s, %s)", realm, uuid)
	// logger.Trace("%+v", cdbFiles)
	maxlevel := len(cdbFiles[realm])
	for level := 1; level < maxlevel; level++ {
		if len(cdbFiles[realm][level]) == 0 {
			continue
		}
		for _, cdb_fn := range cdbFiles[realm][level] {
			db, err := cdb.Open(cdb_fn)
			if err != nil {
				return info, nil, err
			}
			indx, err := db.Data(uuid.Bytes())
			logger.Debugf("findAtLevelHigher(%s, %s) @L%02d %s ? (%s, %s)",
				realm, uuid, level, cdb_fn, indx, err)
			switch err {
			case nil:
				data, err := db.Data(indx)
				db.Close()
				if err != nil {
					logger.Error("cannot get ", indx, " from ", cdb_fn, ": ", err)
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
			logger.Debug("searching ", uuid, ": ", tarfn_b, " ", err)
		}
	}
	if err != nil {
		if err == io.EOF {
			err = NotFound
		}
		return
	}
	if tarfn_b != "" {
		tarfn, ok := tarFiles[realm][tarfn_b]
		logger.Trace("tarfn_b=", tarfn_b, " => ", tarfn, "(", ok, ")")
		if !ok {
			logger.Error("cannot find tarfile for ", tarfn)
			err = NotFound
			return
		}
		info, reader, err = GetFromCdb(uuid, tarfn+".cdb")
		logger.Debug("found ", realm, "/", uuid, " in ",
			tarfn, "(", tarfn_b, "): ", info)
	} else {
		err = NotFound
	}
	return
}

func GetFromCdb(uuid UUID, cdb_fn string) (info Info, reader io.Reader, err error) {
	db, err := cdb.Open(cdb_fn)
	defer db.Close()
	if err != nil {
		logger.Error("cannot open ", cdb_fn, ": ", err)
		return
	}
	data, err := db.Data(uuid.Bytes())
	if err != nil {
		if err == io.EOF || err == NotFound {
			logger.Info("cannot find ", uuid, " in ", cdb_fn)
			err = NotFound
		} else {
			logger.Error("cannot find ", uuid, " in ", cdb_fn, ": ", err)
		}
		return
	}
	if len(data) == 0 {
		logger.Warn("got zero length data from ", cdb_fn, " for ", uuid)
		err = NotFound
		return
	}
	info, err = ReadInfo(bytes.NewReader(data))
	if err != nil {
		logger.Error("cannot read info from ", data, ": ", err)
		return
	}
	if info.Dpos == 0 {
		logger.Warn("got zero Dpos from ", cdb_fn, " for ", uuid)
		err = NotFound
		return
	}
	ocdb := FindLinkOrigin(cdb_fn)
	//logger.Printf("cdb_fn=%s == %s", cdb_fn, ocdb)
	tarfn := ocdb[:len(ocdb)-4]
	reader, err = ReadItem(tarfn, int64(info.Dpos))
	if err != nil {
		logger.Error("GetFromCdb(", uuid, ", ", cdb_fn,
			") -> ReadItem(", tarfn, ", ", info.Dpos, ") error: ", err)
	} else {
		logger.Debug("GetFromCdb found ", uuid, " in ", cdb_fn, ": tarfn=",
			tarfn, ", info=", info)
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
	cacheLock.RLock()
	defer cacheLock.RUnlock()
	if cdbFiles[realm] == nil || len(cdbFiles[realm]) == 0 || len(cdbFiles[realm][0]) == 0 {
		logger.Error("emtpy level zero? ", cdbFiles[realm])
		err = NotFound
		return
	}
	logger.Debugf("L00 files at %s: %d", realm, len(cdbFiles[realm][0]))
	for _, cdb_fn := range cdbFiles[realm][0] {
		info, reader, err = GetFromCdb(uuid, cdb_fn)
		switch err {
		case nil:
			logger.Debugf("L00 found %s in %s: %s", uuid, cdb_fn, info)
			return
		case io.EOF, NotFound:
			continue
		default:
			logger.Errorf("L00 error in GetFromCdb(%s, %s): %s", uuid, cdb_fn, err)
			return info, nil, err
		}
	}

	logger.Debugf("findAtLevelZero(%s, %s): %s", realm, uuid, info)
	return info, nil, NotFound
}

func findAtStaging(uuid UUID, path string) (info Info, reader io.Reader, err error) {
	ifh, err := os.Open(path + "/" + uuid.String() + SuffInfo)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Error("findAtStaging(", uuid, ") other error: ", err)
			return
		}
		return
	}
	logger.Debug("L-1 found ", uuid, " at ", path, " as ", ifh)
	info, err = ReadInfo(ifh)
	ifh.Close()
	if err != nil {
		logger.Error("cannot read info file ", ifh, ": ", err)
		return
	}
	// var suffixes = []string{SuffData + "bz2", SuffData + "gz", SuffLink, SuffData}
	var suffixes = []string{SuffData, SuffLink}
	var fn string
	ce := info.Get("Content-Encoding")
	for _, suffix := range suffixes {
		fn = path + "/" + uuid.String() + suffix
		if fileExists(fn) {
			if suffix == SuffLink {
				fn = FindLinkOrigin(fn)
				if !filepath.IsAbs(fn) {
					fn = path + "/" + fn
				}
				// suffix = fn[strings.LastIndex(fn, "#"):]
				ifn := fn[:len(fn)-1] + "!"
				ifh_o, err := os.Open(ifn)
				if err != nil {
					logger.Errorf("findAtStaging(%s) symlink %s: %s",
						uuid, ifn, err)
					return info, nil, err
				}
				info_o, err := ReadInfo(ifh_o)
				ifh_o.Close()
				if err != nil {
					logger.Error("cannot read symlink info ", ifh_o, ": ", err)
					return info, nil, err
				}
				ce = info_o.Get("Content-Type")
			}
			fh, err := os.Open(fn)
			if err != nil {
				logger.Error("cannot open ", fn, ": ", err)
				return Info{}, nil, err
			}
			switch ce {
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
