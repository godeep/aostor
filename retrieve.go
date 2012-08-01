package aostor

//retrieve

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	"path/filepath"
	"strings"
	"errors"
)

var NotFound = errors.New("Not Found")

func Get(realm string, uuid string) (info Info, reader io.Reader, err error) {
	conf, err := ReadConf("", realm)
	if err != nil {
		logger.Printf("cannot read config: %s", err)
		return
	}
	// L00
	info, reader, err = findAtStaging(uuid, conf.StagingDir)
	if err == nil {
		logger.Printf("found at staging: %s", info)
		return
	} else if !os.IsNotExist(err) {
		logger.Printf("error searching at staging: %s", err)
		return
	}

	info, reader, err = findAtLevelZero(uuid, conf.IndexDir)
	if err == nil {
		logger.Printf("found at level zero: %s", info)
		return
	} else if err != io.EOF {
		return
	}
	var tar_uuid string
	for level := 1; level < 1000 && err == io.EOF; level++ {
		dn := conf.IndexDir + fmt.Sprintf("/L%02d", level)
		if !fileExists(dn) {
			break
		}
		tar_uuid, err = findInCdbs(uuid, dn)
		logger.Printf("searching %s at %s: %s %s", uuid, dn, tar_uuid, err)
	}
	if err != nil {
		return
	}
	if tar_uuid != "" {
		var tarfn string
		tarfn = findTar(tar_uuid, conf.TarDir)
		info, reader, err = GetFromCdb(uuid, tarfn+".cdb")
		logger.Printf("found %s/%s in %s(%s): %s, %s",
			realm, uuid, tarfn, tar_uuid, info, reader)
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

func findAtLevelZero(uuid string, path string) (info Info, reader io.Reader, err error) {
	path = path + "/L00"
	files, err := filepath.Glob(path + "/*.tar.cdb")
	if err != nil {
		logger.Printf("error listing %s: %s", path, err)
		return
	}
	logger.Printf("files at %s: %s", path, files)
	for _, cdb_fn := range files {
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

	logger.Printf("findAtLevelZero(%s, %s): %s", uuid, path, info)
	return info, nil, io.EOF
}

func findInCdbs(uuid string, path string) (string, error) {
	var (
		err error
	)
	files, err := filepath.Glob(path + "/*.cdb")
	if err != nil {
		return "", err
	}
	for _, cdb_fn := range files {
		db, err := cdb.Open(cdb_fn)
		if err != nil {
			return "", err
		}
		tar_uuid_b, err := db.Data(StrToBytes(uuid))
		db.Close()
		switch err {
		case nil:
			return BytesToStr(tar_uuid_b), nil
		case io.EOF:
			continue
		default:
			return "", err
		}
	}

	return "", io.EOF
}

func findTar(uuid string, path string) string {
	end := uuid + ".tar"
	var tarfn string
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if info.IsDir() {
				return filepath.SkipDir
			} else {
				return nil
			}
		} else {
			if strings.HasSuffix(info.Name(), end) {
				tarfn = path + "/" + info.Name()
				return io.EOF
			}
		}
		return nil
	})
	return tarfn
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
		if fi.Mode() & os.ModeSymlink == 0 {
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