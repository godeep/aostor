package aostor

//retrieve

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

var NotFound = errors.New("Not Found")

var cdbFiles = map[string][2][]string{}
var tarFiles = map[string](map[string]string){}
var sigchan chan os.Signal

//returns the associated info and data of a given uuid in a given realm
//  1. checks staging area
//  2. checks level zero (symlinked cdbs in ndx/L00)
//  3. checks higher level (older, too) cdbs in ndx/L01, ndx/L02...
//
//The difference between level zero and higher is the following: at level zero,
//there are the tar files' cdbs (symlinked), and these cdbs contains the info
//(with the position information), ready to serve.
//  At higher levels, the cdbs contains only "/%d" signs (which cdb,
//only a number) and that sign is which zero-level cdb. So at this level an
//additional lookup is required.
func Get(realm string, uuid string) (info Info, reader io.Reader, err error) {
	conf, err := ReadConf("", realm)
	if err != nil {
		logger.Printf("cannot read config: %s", err)
		return
	}
	// L00
	info, reader, err = findAtStaging(uuid, conf.StagingDir)
	if err == nil {
		//logger.Printf("found at staging: %s", info)
		return
	} else if !os.IsNotExist(err) {
		logger.Printf("error searching at staging: %s", err)
		return
	}

	fillCdbCache(realm, conf.IndexDir, false)
	info, reader, err = findAtLevelZero(realm, uuid)
	if err == nil {
		//logger.Printf("found at level zero: %s", info)
		return
	} else if err != NotFound {
		return
	}
	fillTarCache(realm, conf.TarDir, false)
	info, reader, err = findAtLevelHigher(realm, uuid, conf.IndexDir)
	return
}

//fills caches (reads tar files and cdb files, caches path)
func FillCaches(force bool) error {
	config, err := ReadConf("", "")
	if err != nil {
		logger.Printf("cannot read config: %s", err)
		return err
	}
	for _, realm := range config.Realms {
		conf, err := ReadConf("", realm)
		if err != nil {
			logger.Printf("cannot read config: %s", err)
			return err
		}
		fillCdbCache(realm, conf.IndexDir, force)
		fillTarCache(realm, conf.TarDir, force)
	}
	if sigchan == nil {
		sigchan = make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGUSR1)
		go recvChangeSig(sigchan)
	}
	return nil
}

func recvChangeSig(sigchan <-chan os.Signal) {
	for {
		_, ok := <-sigchan
		if !ok {
			break
		}
		logger.Printf("received Change signal, calling FillCaches")
		FillCaches(true)
	}
}

//fills the (cached) cdb files list. Rereads if force is true
func fillCdbCache(realm string, indexdir string, force bool) {
	var cf [2][]string
	if !force {
		cf, ok := cdbFiles[realm]
		if ok && (len(cf[0]) > 0 || len(cf[1]) > 0) {
			return
		}
	}

	pat := indexdir + "/L00/*.cdb"
	files, err := filepath.Glob(pat)
	if err != nil {
		logger.Panicf("cannot list %s: %s", pat, err)
	}
	cdbFiles[realm] = [2][]string{files, make([]string, 0, 100)}
	cf = cdbFiles[realm]

	for level := 1; level < 1000 && err == io.EOF; level++ {
		dn := indexdir + fmt.Sprintf("/L%02d", level)
		if !fileExists(dn) {
			break
		}
		pat = dn + "/*.cdb"
		files, err = filepath.Glob(pat)
		if err != nil {
			logger.Panicf("cannot list %s: %s", pat, err)
		}
		cf[1] = append(cf[1], files...)
	}
	// logger.Printf("fillCdbCache(%s, %s, %s): %+v",
	// 	realm, indexdir, force, cdbFiles)
}

func fillTarCache(realm string, tardir string, force bool) {
	if !force {
		tf, ok := tarFiles[realm]
		if ok && len(tf) > 0 {
			return
		}
	}

	tf := make(map[string]string, 1000)
	filepath.Walk(tardir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if info.IsDir() {
				return filepath.SkipDir
			} else {
				return nil
			}
		} else {
			if strings.HasSuffix(info.Name(), ".tar") {
				bn := info.Name()
				fn := path + "/" + bn
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
	// logger.Printf("fillTarCache(%s, %s, %s): %+v",
	// 	realm, tardir, force, tarFiles)
}

func findAtLevelHigher(realm string, uuid string, tardir string) (info Info, reader io.Reader, err error) {
	var tarfn_b string
	for _, cdb_fn := range cdbFiles[realm][1] {
		db, err := cdb.Open(cdb_fn)
		if err != nil {
			return info, nil, err
		}
		indx, err := db.Data(StrToBytes(uuid))
		switch err {
		case nil:
			data, err := db.Data(indx)
			db.Close()
			if err != nil {
				logger.Printf("cannot get %s from %s: %s", indx, cdb_fn, err)
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
		logger.Printf("searching %s at %s: %s %s", uuid, tardir, tarfn_b, err)
	}
	if err != nil {
		if err == io.EOF {
			err = NotFound
		}
		return
	}
	if tarfn_b != "" {
		tarfn, ok := tarFiles[realm][tarfn_b]
		if !ok {
			logger.Printf("cannot find tarfile for %s!", tarfn)
			err = NotFound
			return
		}
		info, reader, err = GetFromCdb(uuid, tarfn+".cdb")
		logger.Printf("found %s/%s in %s(%s): %s, %s",
			realm, uuid, tarfn, tarfn_b, info, reader)
	} else {
		err = NotFound
	}
	return
}

func GetFromCdb(uuid string, cdb_fn string) (info Info, reader io.Reader, err error) {
	db, err := cdb.Open(cdb_fn)
	defer db.Close()
	if err != nil {
		logger.Printf("cannot open %s: %s", cdb_fn, err)
		return
	}
	data, err := db.Data(StrToBytes(uuid))
	if err != nil {
		logger.Printf("cannot find %s: %s", uuid, err)
		return
	}
	info, err = ReadInfo(bytes.NewReader(data))
	if err != nil {
		logger.Printf("cannot read info from %s: %s", data, err)
		return
	}
	ocdb := FindLinkOrigin(cdb_fn)
	//logger.Printf("cdb_fn=%s == %s", cdb_fn, ocdb)
	tarfn := ocdb[:len(ocdb)-4]
	reader, err = ReadItem(tarfn, int64(info.Dpos))
	logger.Printf("found %s in %s: tarfn=%s, info=%s, err=%s",
		uuid, cdb_fn, tarfn, info, err)
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

func findAtLevelZero(realm string, uuid string) (info Info, reader io.Reader, err error) {
	logger.Printf("files at %s: %s", realm, cdbFiles[realm][0])
	for _, cdb_fn := range cdbFiles[realm][0] {
		info, reader, err = GetFromCdb(uuid, cdb_fn)
		switch err {
		case nil:
			logger.Printf("found %s in %s: %s", uuid, cdb_fn, info)
			return
		case io.EOF:
			continue
		default:
			logger.Printf("error inf GetFromCdb(%s, %s): %s", uuid, cdb_fn, err)
			return info, nil, err
		}
	}

	logger.Printf("findAtLevelZero(%s, %s): %s", realm, uuid, info)
	return info, nil, NotFound
}

func findAtStaging(uuid string, path string) (info Info, reader io.Reader, err error) {
	ifh, err := os.Open(path + "/" + uuid + SuffInfo)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Printf("findAtStaging(%s) other error: %s", uuid, err)
			return
		}
		return
	}
	info, err = ReadInfo(ifh)
	ifh.Close()
	if err != nil {
		return
	}
	var dfh *os.File
	fn := path + "/" + uuid + SuffData + "bz2"
	if fileExists(fn) {
		if dfh, err = os.Open(fn); err != nil {
			return
		}
		reader = bzip2.NewReader(dfh)
	} else {
		fn = path + "/" + uuid + SuffData + "gz"
		if fileExists(fn) {
			if dfh, err = os.Open(fn); err != nil {
				return
			}
			if reader, err = gzip.NewReader(dfh); err != nil {
				return
			}
		} else {
			fn = path + "/" + uuid
			if reader, err = os.Open(fn); err != nil {
				return
			}
		}
	}
	return
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
			logger.Printf("error following link of %s: %s", fn, err)
			break
		}
	}
	return fn
}
