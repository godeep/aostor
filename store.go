package aostor

import (
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"os"
	"unosoft.hu/aostor/compressor"
)

// var StoreCompressMethod = "bzip2"
var StoreCompressMethod = "gzip"

func Put(realm string, info Info, data io.Reader) (key string, err error) {
	conf, err := ReadConf("", realm)
	if err != nil {
		return
	}

	if info.Key == "" || fileExists(conf.StagingDir+"/"+key+SuffInfo) {
		info.Key, err = StrUUID()
		if err != nil {
			return
		}
	}
	if info.Key == "" {
		logger.Panicf("empty key!")
	}
	key = info.Key
	info.Ipos, info.Dpos = 0, 0

	ifh, err := os.OpenFile(conf.StagingDir+"/"+key+SuffInfo, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = ifh.Write(info.Bytes())
	ifh.Close()
	if err != nil {
		return
	}
	end := compressor.ShorterMethod(StoreCompressMethod)
	dfh, err := os.OpenFile(conf.StagingDir+"/"+key+SuffData+end,
		os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = compressor.CompressCopy(dfh, data, StoreCompressMethod)
	dfh.Close()
	fs := fileSize(dfh.Name())
	if fs <= 0 {
		err = errors.New("Empty compressed file!")
	} else {
		// logger.Printf("%s size=%d", dfh.Name(), fs)
	}
	return
}

func StrUUID() (string, error) {
	k, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", *k), nil
}
