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
	"code.google.com/p/go-uuid/uuid"
	"encoding/hex"
	"errors"
	"fmt"
	//"bitbucket.org/taruti/mimemagic"
	"io"
	// "io/ioutil"
	"os"
	"unosoft.hu/aostor/compressor"
)

// var StoreCompressMethod = "bzip2"
var StoreCompressMethod = "gzip"

var UUIDMaker = uuid.NewRandom

// puts file (info + data) into the given realm - returns the key
// if the key is in info, then uses that
func Put(realm string, info Info, data io.Reader) (key string, err error) {
	if err = info.Prepare(); err != nil {
		return "", err
	}
	conf, err := ReadConf("", realm)
	if err != nil {
		return
	}

	if info.Key == "" || fileExists(conf.StagingDir+"/"+key+SuffInfo) {
		b, err := NewUUID()
		if err != nil {
			return "", err
		}
		info.Key = b.String()
	}
	if info.Key == "" {
		logger.Critical("empty key!")
		return
	}
	key = info.Key
	info.Ipos, info.Dpos = 0, 0
	if StoreCompressMethod != "" {
		info.Add("Content-Encoding", StoreCompressMethod)
	}

	// end := compressor.ShorterMethod(StoreCompressMethod)
	// dfh, err := os.OpenFile(conf.StagingDir+"/"+key+SuffData+end,
	dfh, err := os.OpenFile(conf.StagingDir+"/"+key+SuffData,
		os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	hsh := conf.ContentHashFunc()
	cnt := NewCounter()
	r := io.TeeReader(data, io.MultiWriter(hsh, cnt))
	n, err := compressor.CompressCopy(dfh, r, StoreCompressMethod)
	dfh.Close()
	dfh.Sync()

	fs := fileSize(dfh.Name())
	if fs <= 0 {
		err = errors.New("Empty compressed file!")
		return
	} else if n <= 0 || cnt.Num <= 0 {
		err = fmt.Errorf("Empty data (n=%d, cnt=%d)", n, cnt.Num)
		return
	} else {
		// logger.Printf("%s size=%d", dfh.Name(), fs)
	}
	info.Add(InfoPref+"Original-Size", fmt.Sprintf("%d", cnt.Num))
	info.Add(InfoPref+"Stored-Size", fmt.Sprintf("%d", fs))
	info.Add(InfoPref+"Content-"+conf.ContentHash,
		fmt.Sprintf("%x", hsh.Sum(nil)))

	ifh, err := os.OpenFile(conf.StagingDir+"/"+key+SuffInfo, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = ifh.Write(info.Bytes())
	ifh.Close()
	if err != nil {
		return
	}
	return
}

type UUID [16]byte

// returns a hexified 16-byte UUID1
func NewUUID() (UUID, error) {
	var b UUID
	u := UUIDMaker()
	for i := 0; i < 16; i++ {
		b[i] = u[i]
	}
	return b, nil
}

func NewUUIDFromString(text string) (b UUID, err error) {
	u, e := hex.DecodeString(text)
	if e != nil {
		err = e
		return
	}
	for i := 0; i < 16 && i < len(u); i++ {
		b[i] = u[i]
	}
	return
}

func (b UUID) String() string {
	// return fmt.Sprintf("%032x", b)
	return hex.EncodeToString(b[0:])
}
func (b UUID) Bytes() []byte {
	return b[0:]
}

// A writer which counts bytes written into it
type CountingWriter struct {
	Num uint64 // bytes written
}

func NewCounter() *CountingWriter {
	return &CountingWriter{}
}

// just count
func (c *CountingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	c.Num += uint64(n)
	return n, nil
}
