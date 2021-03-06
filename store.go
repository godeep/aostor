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
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/tgulacsi/aostor/uuid"
	//"bitbucket.org/taruti/mimemagic"
	"io"
	"path/filepath"
	// "io/ioutil"
	// "./compressor"
	"github.com/tgulacsi/aostor/compressor"
	"os"
)

var UUIDMaker = uuid.NewUUID4

// puts file (info + data) into the given realm - returns the key
// if the key is in info, then uses that
func Put(realm string, info Info, data io.Reader) (key UUID, err error) {
	if err = info.Prepare(); err != nil {
		return UUID{}, err
	}
	conf, err := ReadConf("", realm)
	if err != nil {
		return
	}

	if info.Key.IsEmpty() {
		info.Key, err = NewUUID()
		if err != nil {
			logger.Critical("empty key! %s", err)
			return
		}
	}
	key = info.Key
	key_s := key.String()
	ifn := filepath.Join(conf.StagingDir, key_s[:2], key_s+SuffInfo)
	if fileExists(ifn) {
		info.Key, err = NewUUID()
		if err != nil {
			return
		}
		key = info.Key
		key_s = key.String()
		ifn = filepath.Join(conf.StagingDir, key_s[:2], key_s+SuffInfo)
	}
	dn := filepath.Dir(ifn)
	if err = os.MkdirAll(dn, 0755); err != nil {
		return
	}
	info.Ipos, info.Dpos = 0, 0
	if conf.CompressMethod != "" {
		info.Add("Content-Encoding", conf.CompressMethod)
	}

	// end := compressor.ShorterMethod(StoreCompressMethod)
	dfn := ifn[:len(ifn)-len(SuffInfo)] + SuffData
	dfh, err := os.OpenFile(dfn, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	hsh := conf.ContentHashFunc()
	cnt := NewCounter()
	r := io.TeeReader(data, io.MultiWriter(hsh, cnt))
	n, err := compressor.CompressCopy(dfh, r, conf.CompressMethod)
	_ = dfh.Close()
	_ = dfh.Sync()

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

	ifh, err := os.OpenFile(ifn, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return
	}
	_, err = ifh.Write(info.Bytes())
	_ = ifh.Close()
	if err != nil {
		return
	}
	return
}

type UUID [uuid.Length]byte

// returns a hexified uuid.Length-byte UUID1
func NewUUID() (UUID, error) {
	var b UUID
	u := UUIDMaker()
	for i := 0; i < uuid.Length; i++ {
		b[i] = u[i]
	}
	return b, nil
}

func UUIDFromString(text string) (b UUID, err error) {
	var u []byte
	switch n := len(text); n {
	case 2 * uuid.Length:
		u, err = hex.DecodeString(text)
	case 22, 23, 24:
		// logger.Debugf("UUIDFromString(%s + %s [%d])", text, "=="[:24-n], len(text))
		u, err = base64.URLEncoding.DecodeString(text + "=="[:24-n])
	default:
		err = fmt.Errorf("Invalid length %d of %s", len(text), text)
	}
	if err != nil {
		return
	}
	for i := 0; i < uuid.Length && i < len(u); i++ {
		b[i] = u[i]
	}
	return
}
func UUIDFromBytes(text []byte) (b UUID, err error) {
	switch len(text) {
	case 2 * uuid.Length, 22, 23, 24:
		return UUIDFromString(string(text))
	}
	for i := 0; i < uuid.Length && i < len(text); i++ {
		b[i] = text[i]
	}
	return
}

func (b UUID) String() string {
	// return hex.EncodeToString(b[0:])
	return base64.URLEncoding.EncodeToString(b[0:])[:22] //strip padding ==
}
func (b UUID) Bytes() []byte {
	return b[0:]
}
func (b UUID) IsEmpty() bool {
	for i := 0; i < uuid.Length; i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
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
