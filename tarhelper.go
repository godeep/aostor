//!/usr/bin/env go
package aodb

import (
	"archive/tar"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"errors"
	//"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/user"
	"strings"
	"time"
)

var (
	ErrSymlink     = errors.New("aodb/tarhelper: symlink")
	NotRegularFile = errors.New("aodb/tarhelper: not a regular file")
	ErrBadTarEnd   = errors.New("aodb/tarhelper: bad tar end")
)

var logger = log.New(os.Stderr, "tarhelper ", log.LstdFlags|log.Lshortfile)

const (
	SuffInfo = "!"
	SuffLink = "@"
	SuffData = "#"
	BS       = 512
)

type SymlinkError struct {
	Linkname string
}

func (e *SymlinkError) Error() string {
	return "symlink found: " + e.Linkname
}

// readItem reads from
func readItem(tarfn string, pos int64) (ret io.Reader, err error) {
	if f, err := os.Open(tarfn); err == nil {
		defer f.Close()
		f.Seek(pos, 0)
		tr := tar.NewReader(f)
		if hdr, err := tr.Next(); err == nil {
			switch {
			case hdr.Typeflag == tar.TypeSymlink:
				err = &SymlinkError{hdr.Linkname}
			case hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA:
				err = NotRegularFile
			case strings.HasSuffix(hdr.Name, SuffData+"bz2"):
				ret = bzip2.NewReader(io.LimitReader(tr, hdr.Size))
			case strings.HasSuffix(hdr.Name, SuffData+"gz"):
				ret, err = gzip.NewReader(io.LimitReader(tr, hdr.Size))
			case true:
				ret = tr
			}
		}
	}
	return
}

// writeItem writes the given io
func writeItem(tarfn string, fn string) (pos uint64, err error) {
	pos = 0
	sfh, err := os.Open(fn)
	defer sfh.Close()
	if sfi, err := sfh.Stat(); err == nil {
		hdr := new(tar.Header)
		hdr.Name = sfi.Name()
		hdr.Size = sfi.Size()
		hdr.Mode = int64(sfi.Mode().Perm())
		hdr.ModTime = sfi.ModTime()
		var (
			tw *tar.Writer
			f  ReadWriteSeekCloser
		)
		if tw, f, pos, err = openForAppend(tarfn); err == nil {
			defer f.Close()
			defer tw.Close() //LIFO
			if err := tw.WriteHeader(hdr); err == nil {
				if zw, err := gzip.NewWriterLevel(tw, flate.BestCompression); err == nil {
					defer zw.Close()
					_, err = io.Copy(zw, sfh)
				}
			}
		}
	}
	return
}

type ReadWriteSeekCloser interface {
	io.ReadWriteSeeker
	io.Closer
}

/*
   def _init_find_end(self, end_offset, name):
       '''Move to the end of the archive, before the first empty block.'''
       if not end_offset:
           end_offset = self.END_OFFSET_CACHE.get(name, 0)

       self.firstmember = None
       perf_mark()
       if end_offset > self.offset:
           self.fileobj.seek(0, 2)
           p = self.fileobj.tell()
           self.offset = min(p - tarfile.BLOCKSIZE, end_offset)
           self.fileobj.seek(self.offset)
           perf_print('%s end_offset > self.offset', name)

       if DEBUG_MEM:
           LOG.debug('before while: next() mem=%dKb', usedmem())
       while True:
           if self.next() is None:
               if self.offset > 0:
                   self.fileobj.seek(-tarfile.BLOCKSIZE, 1)
               break
       perf_print('find_end %s', name)
       self.END_OFFSET_CACHE[name] = self.offset
       if DEBUG_MEM:
           LOG.debug('after while: next() mem=%dKb', usedmem())
*/
//Move to the end of the archive, before the first empty block.
func FindTarEnd(r io.ReadSeeker, last_known uint64) (pos uint64, err error) {
	var p int64
	if p, err = r.Seek(0, 1); err != nil {
		logger.Panicf("cannot seek %s: %s", r, err)
	}
	logger.Printf("p=%d last_known=%d", p, last_known)
	if last_known > uint64(p) {
		p, err = r.Seek(-BS, 2)
		if uint64(p) > last_known {
			if p, err = r.Seek(int64(last_known), 0); err != nil {
				logger.Panicf("cannot seek to %d", int64(last_known))
			}
		}
		logger.Printf("p=%d", p)
	}
	tr := tar.NewReader(r)
	for {
		if _, err := tr.Next(); err == io.EOF {
			p, err = r.Seek(-2*BS, 1)
			break
		} else {
			// p, err = r.Seek(0, 1); logger.Printf("pos=%d", p)
		}
	}
	logger.Printf("end of %s: %d", r, p)
	return uint64(p), err
}

func openForAppend(tarfn string) (
	tw *tar.Writer, fobj ReadWriteSeekCloser, pos uint64, err error) {
	fh, err := os.OpenFile(tarfn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		logger.Printf("cannot open %s: %s", tarfn, err)
		return
	}
	fi, err := fh.Stat()
	if err != nil {
		logger.Printf("cannot stat %s: %s", fh, err)
		return
	}
	logger.Printf("%s.Size=%d", tarfn, fi.Size())
	var p int64
	if fi.Size() >= 2*BS {
		if pos, err = FindTarEnd(fh, 0); err == nil {
			logger.Printf("end of %s: %d", tarfn, pos)
		} else {
			logger.Printf("error %s: %s", tarfn, err)
		}
	} else {
		if p, err = fh.Seek(0, 0); err != nil {
			logger.Printf("error: %s: %s", tarfn, err)
		}
		pos = uint64(p)
	}
	tw = tar.NewWriter(fh)
	if err == nil && tw == nil {
		logger.Panicf("couldn't open %+v!", tarfn)
	}
	fobj = fh
	logger.Printf("opened %s (err=%s): tw=%+v, fh=%+v, pos=%d",
		tarfn, err, tw, fh, pos)
	return
}

type Info map[string]string

func (info Info) Get(key string) string {
	ret, ok := info[http.CanonicalHeaderKey(key)]
	if !ok {
		for k := range info {
			k2 := http.CanonicalHeaderKey(k)
			if k != k2 {
				info[k2] = info[k]
				delete(info, k)
			}
		}
		ret = info[http.CanonicalHeaderKey(key)]
	}
	return ret
}
func (info Info) Add(key string, val string) {
	info[http.CanonicalHeaderKey(key)] = val
}

func (info Info) NewReader() (io.Reader, int) {
	buf := make([]string, len(info))
	i, n := 0, 0
	for k, v := range info {
		buf[i] = fmt.Sprintf("%s: %s", http.CanonicalHeaderKey(k), v)
		n += len(buf[i]) + 1
		i++
	}
	logger.Printf("info=%+v", info)
	return strings.NewReader(strings.Join(buf, "\n")), n - 1
}
func (info Info) Bytes() []byte {
	r, _ := info.NewReader()
	ret, err := ioutil.ReadAll(r)
	if err != nil {
		logger.Panicf("cannot read back: %s", err)
	}
	return ret
}

func fileTarHeader(fn string) (hdr *tar.Header, err error) {
	if fi, err := os.Stat(fn); err == nil {
		m := fi.Mode()
		var (
			ln string
			tm byte
		)
		tm = tar.TypeReg
		switch {
		case m&os.ModeSymlink != 0:
			tm = tar.TypeSymlink
			if lfi, err := os.Lstat(fn); err == nil {
				ln = lfi.Name()
			}
		case m&os.ModeDevice != 0 && m&os.ModeCharDevice != 0:
			tm = tar.TypeChar
		case m&os.ModeDevice != 0:
			tm = tar.TypeBlock
		case m&os.ModeNamedPipe != 0 || m&os.ModeSocket != 0:
			tm = tar.TypeFifo
		}
		tim := fi.ModTime()
		hdr = &tar.Header{Name: fi.Name(), Mode: int64(m.Perm()),
			Size: fi.Size(), ModTime: tim,
			Typeflag: tm, Linkname: ln}
		FillHeader(hdr)
	}
	return
}

func FillHeader(hdr *tar.Header) {
	var cuname string
	cuid := os.Getuid()
	if curr, err := user.LookupId(fmt.Sprintf("%d", cuid)); err == nil {
		cuname = curr.Username
	}
	if hdr.Uid == 0 {
		if hdr.Uname == "" {
			hdr.Uid = cuid
			hdr.Uname = cuname
		} else {
			if usr, err := user.Lookup(hdr.Uname); err == nil {
				if i, err := fmt.Sscanf("%d", usr.Uid); err == nil {
					hdr.Uid = i
					hdr.Uname = usr.Username
				}
			}
		}
	}
	if hdr.Gid == 0 {
		if hdr.Gname == "" {
			if hdr.Uid != 0 {
				if usr, err := user.LookupId(fmt.Sprintf("%d", hdr.Uid)); err == nil {
					if i, err := fmt.Sscanf("%d", usr.Gid); err == nil {
						hdr.Gid = i
					}
				}
			}
		}
	}
	if hdr.ModTime.IsZero() {
		hdr.ModTime = time.Now()
	}
	if hdr.AccessTime.IsZero() {
		hdr.AccessTime = hdr.ModTime
	}
	if hdr.ChangeTime.IsZero() {
		hdr.ChangeTime = hdr.ModTime
	}
}

func writeTar(tw *tar.Writer, hdr *tar.Header, r io.Reader) (err error) {
	if err = tw.WriteHeader(hdr); err != nil {
		logger.Panicf("error writing tar header %+v into %+v: %s", hdr, tw, err)
	}
	if n, err := io.Copy(tw, r); err != nil {
		logger.Panicf("error copying tar data %+v into %+v: %s", r, tw, err)
	} else {
		logger.Printf("written %+v, then %d bytes into %+v", hdr, n, tw)
	}
	tw.Flush()
	return
}

func writeInfo(tw *tar.Writer, info Info) (err error) {
	//logger.Printf("writeInfo")
	txt, length := info.NewReader()
	//logger.Printf("txt=%s", txt)
	hdr := &tar.Header{Name: info.Get("X-AODB-Id") + SuffInfo, Mode: 0440,
		Size: int64(length), Typeflag: tar.TypeReg}
	FillHeader(hdr)
	logger.Printf("writeInfo(%+v into %+v)", hdr, tw)
	return writeTar(tw, hdr, txt)
}

func writeCompressed(tw *tar.Writer, fn string, info Info,
	compressMethod string) (err error) {
	if sfh, err := os.Open(fn); err == nil {
		defer sfh.Close()
		if cfn, err := Compress(sfh, compressMethod); err == nil {
			logger.Printf("compressed file: %s", cfn)
			defer os.Remove(cfn)
			if cfh, err := os.Open(cfn); err == nil {
				if fi, err := cfh.Stat(); err == nil {
					defer cfh.Close()
					var end = compressMethod
					switch end {
					case "gzip":
						end = "gz"
					case "bzip2":
						end = "bz2"
					}
					if hdr, err := fileTarHeader(fn); err == nil {
						hdr.Name = info.Get("X-AODB-Id") + SuffData + end
						hdr.Size = fi.Size()
						hdr.Mode = 0400
						err = writeTar(tw, hdr, cfh)
					}
				}
			}
		}
	}
	if err != nil {
		logger.Panicf("couldn't write %s into %s: %s", fn, tw, err)
	}
	return
}

// AppendFile appends file fn with info
func AppendFile(tarfn string, info Info, fn string, compressMethod string) (pos uint64, err error) {
	tw, fh, pos, err := openForAppend(tarfn)
	if err != nil {
		logger.Printf("openForAppend(%s): %s", tarfn, err)
		return
	}
	defer fh.Close()
	defer tw.Close()
	defer tw.Flush()
	if fi, err := os.Stat(fn); err == nil {
		info.Add("X-AODB-Original-Size", fmt.Sprintf("%d", fi.Size()))
		info.Add("X-AODB-Original-Name", fn)

		//pos, _ := fh.Seek(0, 1); logger.Printf("B %+v pos: %d", fh, pos)
		if err = writeInfo(tw, info); err == nil {
			// pos, _ = fh.Seek(0, 1); logger.Printf("C %+v pos: %d", fh, pos)
			err = writeCompressed(tw, fn, info, compressMethod)
			// pos, _ = fh.Seek(0, 1); logger.Printf("D %+v pos: %d", fh, pos)
		}
	}
	if err != nil {
		logger.Printf("AppendFile: %s", err)
	} else {
		// pos, _ := fh.Seek(0, 1); logger.Printf("E %+v pos: %d", fh, pos)
		// tw.Flush()
		// pos, _ = fh.Seek(0, 1); logger.Printf("F %+v pos: %d", fh, pos)
		// tw.Close()
		// pos, _ = fh.Seek(0, 1); logger.Printf("G %+v pos: %d", fh, pos)
	}
	return
}

// AppendLink appends as link pointing at a previous item
func AppendLink(tarfn string, info Info, src string, dst string) (err error) {
	//var (twp *tar.Writer; fh ReadWriteSeekCloser; pos uint64)
	if tw, fh, _, err := openForAppend(tarfn); err == nil {
		defer fh.Close()
		defer tw.Close()

		if err := writeInfo(tw, info); err == nil {
			hdr := new(tar.Header)
			hdr.Name = src
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = dst
			if err := tw.WriteHeader(hdr); err == nil {
				tw.Flush()
			}
		}
	}
	return
}

func Compress(f *os.File, compressMethod string) (tempfn string, err error) {
	tempfn = os.TempDir() + fmt.Sprintf("/tarhelper-%s-%d.gz", f.Name(), os.Getpid())
	if fh, err := os.OpenFile(tempfn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600); err == nil {
		defer fh.Close()
		//logger.Printf("fh=%v", fh)
		if gw, err := gzip.NewWriterLevel(fh, flate.BestCompression); err == nil {
			defer gw.Close()
			//logger.Printf("gw=%v", gw)
			if _, err := io.Copy(gw, f); err != nil {
				//logger.Printf("copied %d bytes from %v to %v", n, f, gw)
				//} else {
				logger.Printf("copy from %v to %v error: %s", f, gw, err)
			}
		}
	}
	return
}
