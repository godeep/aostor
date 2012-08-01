package aostor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

const InfoPref = "X-Aostor-" // prefix of specific headers

var MissingFilenameError = errors.New("Filename is missing!")

type Info struct {
	Key        string
	Ipos, Dpos uint64
	m          map[string]string
}

// returns the value for key
func (info *Info) Get(key string) string {
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

// adds a key
func (info *Info) Add(key string, val string) {
	k := http.CanonicalHeaderKey(key)
	val = strings.TrimSpace(val)
	if info.m == nil {
		info.m = make(map[string]string, 1)
	}
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

// sets the filename and the mimetype, conditionally (if given)
func (info *Info) SetFilename(fn string, mime string) {
	if fn != "" {
		fn = BaseName(fn)
		info.Add(InfoPref+"Original-Filename", fn)
		info.Add("Content-Disposition", `attachment; filename="`+fn+`"`)
	}
	if mime != "" {
		info.Add("Content-Type", mime)
	}
}

// copies adata from Info to http.Header
func (info *Info) Copy(header http.Header) {
	for k, v := range info.m {
		header.Add(k, v)
	}
}

// parses into Info
func ReadInfo(r io.Reader) (info Info, err error) {
	rb := bufio.NewReader(r)
	var key, val string
	if info.m == nil {
		info.m = make(map[string]string, 3)
	}
	for err == nil {
		if key, err = rb.ReadString(':'); err == nil {
			if val, err = rb.ReadString('\n'); err == nil {
				info.Add(key[:len(key)-1], val[:len(val)-1])
			}
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// returns a new Reader and the length for wire format of Info
func (info *Info) NewReader() (io.Reader, int) {
	buf := make([]string, len(info.m)+3)
	i := 0
	info.Prepare()

	//length := 0
	for k, v := range info.m {
		//if !strings.HasPrefix(k, InfoPref) || len(k) > len(InfoPref) {
		buf[i] = fmt.Sprintf("%s: %s", http.CanonicalHeaderKey(k), v)
		//length += len(buf[i]) + 1
		i++
		//}
	}
	text := strings.Join(buf, "\n")
	//logger.Printf("info[%d]=%s", len(text), text)
	if len(text) <= 2 {
		logger.Panicf("empty info (key=%s m=%+v)", info.Key, info.m)
	}
	return strings.NewReader(text), len(text)
}

// returns Info in wire format
func (info *Info) Bytes() []byte {
	r, _ := info.NewReader()
	ret, err := ioutil.ReadAll(r)
	if err != nil {
		logger.Panicf("cannot read back: %s", err)
	}
	return ret
}

// prepares info for writing out
func (info *Info) Prepare() error {
	if info.Key != "" {
		info.Add(InfoPref+"Id", info.Key)
		//logger.Printf("added %s => %+v info.m nil? %s", info.Key, info.m, info.m == nil)
	}
	if info.Ipos > 0 {
		info.Add(InfoPref+"Ipos", fmt.Sprintf("%d", info.Ipos))
	}
	if info.Dpos > 0 {
		info.Add(InfoPref+"Dpos", fmt.Sprintf("%d", info.Dpos))
	}

	if info.m == nil {
		info.m = make(map[string]string, 3)
	}
	fn := BaseName(info.m[InfoPref+"Original-Filename"])
	if fn == "" && info.m["Content-Disposition"] != "" {
		cd := info.m["Content-Disposition"]
		if strings.Contains(cd, "filename=") {
			fn = BaseName(strings.Trim(strings.SplitAfter(cd, "filename=")[1], `"'`))
		}
		info.m[InfoPref+"Original-Filename"] = fn
	} else if info.m["Content-Disposition"] == "" {
		info.m["Content-Disposition"] = `attachment; filename="` + fn + `"`
	}
	if fn != "" && info.m["Content-Type"] == "" {
		splitted := strings.Split(fn, ".")
		if len(splitted) > 1 {
			info.m["Content-Type"] = mime.TypeByExtension("." + splitted[len(splitted)-1])
		}
	}
	if fn == "" {
		return MissingFilenameError
	}
	return nil
}

// convert string to []byte
func StrToBytes(str string) []byte {
	return bytes.NewBufferString(str).Bytes()
}

// converts []byte to string
func BytesToStr(buf []byte) string {
	return bytes.NewBuffer(buf).String()
}

// basename for Windows and Unix (strips everything before / or \\
func BaseName(fn string) string {
	p := strings.LastIndexAny(fn, `/\`)
	if p >= 0 {
		fn = fn[p+1:]
	}
	return fn
}
