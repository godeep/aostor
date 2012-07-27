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
)

func Get(realm string, uuid string) (info Info, reader io.Reader, err error) {
	conf, err := ReadConf("", realm)
	if err != nil {
		return
	}
	// L00
	info, reader, err = findAtStaging(uuid, conf.StagingDir)
	if err == nil {
		return
	} else if err != io.EOF {
		return
	}

	info, reader, err = findAtLevelZero(uuid, conf.IndexDir)
	if err == nil {
		return
	} else if err != io.EOF {
		return
	}
	var tar_uuid string
	for level := 1; level < 10 && err == io.EOF; level++ {
		tar_uuid, err = findInCdbs(uuid, conf.IndexDir+fmt.Sprintf("/L%02d", level))
	}
	if err != nil {
		return
	}
	if tar_uuid != "" {
		var tarfn string
		tarfn = findTar(tar_uuid, conf.TarDir)
		info, reader, err = GetFromCdb(uuid, tarfn+".cdb")
	}
	return
}

func GetFromCdb(uuid string, cdb_fn string) (info Info, reader io.Reader, err error) {
	db, err := cdb.Open(cdb_fn)
	defer db.Close()
	if err != nil {
		return
	}
	data, err := db.Data(StrToBytes(uuid))
	if err != nil {
		return
	}
	info, err = ReadInfo(bytes.NewReader(data))
	if err != nil {
		return
	}
	tarfn := cdb_fn[:len(cdb_fn)-4]
	reader, err = ReadItem(tarfn, int64(info.Dpos))
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
	files, err := filepath.Glob(path + "/*.tar.cdb")
	if err != nil {
		return
	}
	for _, cdb_fn := range files {
		info, reader, err = GetFromCdb(uuid, cdb_fn)
		switch err {
		case nil:
			break
		case io.EOF:
			continue
		default:
			return info, nil, err
		}
	}

	return
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
			return
		}
		err = nil
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
