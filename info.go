package aostor

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"fmt"
	)
const InfoPref = "X-Aostor-"

type Info struct {
	Key        string
	Ipos, Dpos uint64
	m          map[string]string
}

func (info Info) Get(key string) string {
	ret, ok := info.m[http.CanonicalHeaderKey(key)]
	if !ok {
		for k := range info.m {
			k2 := http.CanonicalHeaderKey(k)
			if k != k2 {
				info.m[k2] = info.m[k]
				delete(info.m, k)
			}
		}
		ret = info.m[http.CanonicalHeaderKey(key)]
	}
	return ret
}
func (info Info) Add(key string, val string) {
	k := http.CanonicalHeaderKey(key)
	val = strings.TrimSpace(val)
	info.m[k] = val
	if val != "" && strings.HasPrefix(k, InfoPref) {
		switch k[len(InfoPref):] {
		case "Id":
			info.Key = val
		case "Ipos":
			info.Ipos, _ = strconv.ParseUint(val, 0, 64)
		case "Dpos":
			info.Dpos, _ = strconv.ParseUint(val, 0, 64)
		}
	}
}

func ReadInfo(r io.Reader) (info Info, err error) {
	rb := bufio.NewReader(r)
	var key, val string
	if info.m == nil {
		info.m = make(map[string]string)
	}
	for err == nil {
		if key, err = rb.ReadString(':'); err == nil {
			if val, err = rb.ReadString('\n'); err == nil {
				info.Add(key[:len(key)-1], val[:len(val)-1])
			}
		}
	}
	return
}

func (info Info) NewReader() (io.Reader, int) {
	buf := make([]string, len(info.m)+3)
	i := 0
	if info.Key != "" {
		info.Add(InfoPref+"Id", info.Key)
	}
	if info.Ipos > 0 {
		info.Add(InfoPref+"Ipos", fmt.Sprintf("%d", info.Ipos))
	}
	if info.Dpos > 0 {
		info.Add(InfoPref+"Dpos", fmt.Sprintf("%d", info.Dpos))
	}
	for k, v := range info.m {
		if !strings.HasPrefix(k, InfoPref) || len(k) > len(InfoPref) {
			buf[i] = fmt.Sprintf("%s: %s", http.CanonicalHeaderKey(k), v)
			i++
		}
	}
	text := strings.Join(buf, "\n")
	logger.Printf("info[%d]=%s", len(text), text)
	return strings.NewReader(text), len(text)
}
func (info Info) Bytes() []byte {
	r, _ := info.NewReader()
	ret, err := ioutil.ReadAll(r)
	if err != nil {
		logger.Panicf("cannot read back: %s", err)
	}
	return ret
}

func StrToBytes(str string) []byte {
	return bytes.NewBufferString(str).Bytes()
}

/*
func StrToBytes(txt string) (ret []byte) {
	ret, err := ioutil.ReadAll(strings.NewReader(txt))
	if err != nil {
		logger.Panicf("cannot read back: %s", err)
	}
	return
}
*/

func BytesToStr(buf []byte) string {
	return bytes.NewBuffer(buf).String()
}
