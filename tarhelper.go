//!/usr/bin/env go
package aodb

import (
	"archive/tar"
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	ErrSymlink     = errors.New("aodb/tarhelper: symlink")
	NotRegularFile = errors.New("aodb/tarhelper: not a regular file")
	ErrBadTarEnd   = errors.New("aodb/tarhelper: bad tar end")
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
			case strings.HasSuffix(hdr.Name, "#bz2"):
				ret = bzip2.NewReader(io.LimitReader(tr, hdr.Size))
			case strings.HasSuffix(hdr.Name, "#gz"):
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
			defer tw.Close()
			defer f.Close()
			if err := tw.WriteHeader(hdr); err == nil {
				if zw, err := gzip.NewWriterLevel(tw, 9); err == nil {
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

func openForAppend(tarfn string) (tw *tar.Writer, fh ReadWriteSeekCloser, pos uint64, err error) {
	fh, err = os.OpenFile(tarfn, os.O_CREATE|os.O_APPEND, 0640)
	if err == nil {
		fh.Seek(-1024, 2)
		var buf []byte
		if n, err := io.ReadAtLeast(fh, buf, 1024); err == nil {
			if n >= 1024 {
				i := 0
				for i := 0; i < n && buf[i] == 0; i++ {
				}
				if i < 1024 {
					//err
				}
			}
			if p, err := fh.Seek(-1024, 2); err == nil {
				pos = uint64(p)
				tw = tar.NewWriter(fh)
			}
		}
	}
	return
}

func HeaderBytes(info http.Header) []byte {
	r, w := io.Pipe()
	info.Write(bufio.NewWriterSize(w, 1024))
	if buf, err := ioutil.ReadAll(r); err == nil {
		return buf
	}
	return nil
}

func writeInfo(tw tar.Writer, info *http.Header) (err error) {
	txt := HeaderBytes(*info)
	hdr := &tar.Header{Name: (*info).Get("X-Id") + ".info", Mode: 0640,
		Size:    int64(len(txt)),
		ModTime: time.Now(), Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err == nil {
		tw.Write(txt)
		tw.Flush()
	}
	return
}

func writeCompressed(tw tar.Writer, fn string, compressMethod string) (err error) {
	if fi, err := os.Stat(fn); err == nil {
		if sfh, err := os.Open(fn); err == nil {
			defer sfh.Close()
			if cfn, err := Compress(sfh, compressMethod); err == nil {
				defer os.Remove(cfn)
				if cfh, err := os.Open(cfn); err == nil {
					defer cfh.Close()
					hdr := new(tar.Header)
					hdr.Name = fi.Name() + "#" + compressMethod
					hdr.Mode = int64(fi.Mode().Perm())
					hdr.Size = fi.Size()
					hdr.Typeflag = tar.TypeReg
					io.Copy(&tw, cfh)
					tw.Flush()
				}
			}
		}
	}
	return
}

// AppendFile appends file fn with info
func AppendFile(tarfn string, info http.Header, fn string, compressMethod string) (pos uint64, err error) {
	var (
		twp *tar.Writer
		fh  ReadWriteSeekCloser
	)
	if twp, fh, pos, err = openForAppend(tarfn); err != nil {
		return
	}
	tw := *twp
	defer tw.Close()
	defer fh.Close()
	if fi, err := os.Stat(fn); err == nil {
		info.Add("X-Original-Size", fmt.Sprintf("%d", fi.Size()))

		if err = writeInfo(tw, &info); err == nil {
			err = writeCompressed(tw, fn, compressMethod)
		}
	}

	return
}

// AppendLink appends as link pointing at a previous item
func AppendLink(tarfn string, info http.Header, src string, dst string) (err error) {
	//var (twp *tar.Writer; fh ReadWriteSeekCloser; pos uint64)
	if twp, fh, _, err := openForAppend(tarfn); err == nil {
		tw := *twp
		defer tw.Close()
		defer fh.Close()

		if err := writeInfo(tw, &info); err == nil {
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
	tempfn = os.TempDir() + fmt.Sprintf("/tarhelper-%s-%d", f.Name(), os.Getpid())
	if fh, err := os.OpenFile(tempfn, os.O_CREATE|os.O_TRUNC, 0600); err == nil {
		defer fh.Close()
		if gw, err := gzip.NewWriterLevel(fh, flate.BestCompression); err == nil {
			defer gw.Close()
			io.Copy(gw, f)
		}
	}
	return
}
