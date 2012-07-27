package aostor

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"os"
)

func Put(info Info, data io.Reader) (key string, err error) {
	conf, err := ReadConf("")
	if err != nil {
		return
	}
	staging_dir, err := conf.GetString("dirs", "staging")
	if err != nil {
		return
	}

	if info.Key == "" || fileExists(staging_dir+"/"+key+SuffInfo) {
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

	ifh, err := os.OpenFile(staging_dir+"/"+key+SuffInfo, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = ifh.Write(info.Bytes())
	ifh.Close()
	if err != nil {
		return
	}
	dfh, err := os.OpenFile(staging_dir+"/"+key+SuffData+"gz", os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = CompressCopy(dfh, data)
	dfh.Close()
	return
}

func StrUUID() (string, error) {
	k, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", *k), nil
}
