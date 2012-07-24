package aostor

//retrieve

import (
	"io"
	"os"
	//"fmt"
	"compress/bzip2"
	"compress/gzip"
)

func Retrieve(uuid string) (info Info, reader io.Reader, err error) {
	//is it in the temporary area?
	conf, err := ReadConf("")
	if err != nil {
		return
	}
	staging_dir, err := conf.GetString("dirs", "staging")
	if err != nil {
		return
	}
	ifh, err := os.Open(staging_dir + "/" + uuid + SuffInfo)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
		//start the search on level 0
	} else {
		if info, err = ReadInfo(ifh); err != nil {
			return
		}
		var dfh *os.File
		fn := staging_dir + "/" + uuid + SuffData + "bz2"
		if fileExists(fn) {
			if dfh, err = os.Open(fn); err != nil {
				return
			}
			reader = bzip2.NewReader(dfh)
		} else {
			fn = staging_dir + "/" + uuid + SuffData + "gz"
			if fileExists(fn) {
				if dfh, err = os.Open(fn); err != nil {
					return
				}
				if reader, err = gzip.NewReader(dfh); err != nil {
					return
				}
			} else {
				fn = staging_dir + "/" + uuid
				if reader, err = os.Open(fn); err != nil {
					return
				}
			}
		}
	}

	return
}

func fileExists(fn string) bool {
	fh, err := os.Open(fn)
	if err == nil {
		fh.Close()
		return true
	} else {
		return os.IsNotExist(err)
	}
	return false
}
