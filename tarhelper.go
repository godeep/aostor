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
	"archive/tar"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strings"
	"time"
	"unosoft.hu/aostor/compressor"
)

var (
	ErrSymlink     = errors.New("aodb/tarhelper: symlink")
	NotRegularFile = errors.New("aodb/tarhelper: not a regular file")
	ErrBadTarEnd   = errors.New("aodb/tarhelper: bad tar end")
)

const (
	SuffInfo = "!" // suffix of info file
	SuffLink = "@" // suffix of link
	SuffData = "#" // suffix of data file (+ compression type)
	BS       = 512 // tar blocksize
)

var tarEndCache = map[string]uint64{}

type SymlinkError struct {
	Linkname string
}

func (e *SymlinkError) Error() string {
	return "symlink found: " + e.Linkname
}

// Reads from tarfn at starting at pos
// returns a SymplinkError with the symlink information,
// if there is a symlink at the given position
// - to be able to retry with the symlink
func ReadItem(tarfn string, pos int64) (ret io.Reader, err error) {
	f, err := os.Open(tarfn)
	if err != nil {
		logger.Error("cannot open %s: %s", tarfn, err)
	}
	defer f.Close()
	p, err := f.Seek(pos, 0)
	if err != nil {
		logger.Error("cannot seek in %s to %d: %s", f, pos, err)
		return nil, err
	} else if p != pos {
		logger.Error("cannot seek in %s to %d: got %d", f, pos, p)
	}
	tr := tar.NewReader(f)
	hdr, err := tr.Next()
	if err != nil {
		logger.Error("cannot go to next tar header: %s", err)
	}
	logger.Debug("ReadItem(%s, %d) hdr=%s", tarfn, pos, hdr)
	switch {
	case hdr.Typeflag == tar.TypeSymlink:
		err = &SymlinkError{hdr.Linkname}
	case hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA:
		err = NotRegularFile
	// TODO: cut decompression if not used
	case strings.HasSuffix(hdr.Name, SuffData+"bz2"):
		logger.Trace("bz[%s] length=%d", hdr.Name, hdr.Size)
		ret = bzip2.NewReader(io.LimitReader(tr, hdr.Size))
	case strings.HasSuffix(hdr.Name, SuffData+"gz"):
		logger.Trace("gz[%s] length=%d", hdr.Name, hdr.Size)
		ret, err = gzip.NewReader(io.LimitReader(tr, hdr.Size))
	case true:
		logger.Trace("[%s] length=%d", hdr.Name, hdr.Size)
		ret = tr
	}
	//logger.Printf("ret=%s err=%s", ret, err)
	return ret, err
}

// Writes the given file into tarfn
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
		if tw, f, pos, err = OpenForAppend(tarfn); err == nil {
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

// Reader + Writer + Seeker + Closer
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
		logger.Critical("cannot seek %s: %s", r, err)
		return
	}
	logger.Debug("p=%d last_known=%d", p, last_known)
	if last_known > uint64(p) {
		p, err = r.Seek(-BS, 2)
		if uint64(p) > last_known {
			if p, err = r.Seek(int64(last_known), 0); err != nil {
				logger.Critical("cannot seek to %d", int64(last_known))
				return
			}
		}
		logger.Trace("p=%d", p)
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
	logger.Debug("end of %s: %d", r, p)
	return uint64(p), err
}

// Opens the tarfile for appending - seeks to the end
func OpenForAppend(tarfn string) (
	tw *tar.Writer, fobj ReadWriteSeekCloser, pos uint64, err error) {
	fh, err := os.OpenFile(tarfn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		logger.Error("cannot open %s: %s", tarfn, err)
		return
	}
	fi, err := fh.Stat()
	if err != nil {
		logger.Error("cannot stat %s: %s", fh, err)
		return
	}
	//logger.Printf("%s.Size=%d", tarfn, fi.Size())
	var p int64
	if fi.Size() >= 2*BS {
		if pos, err = FindTarEnd(fh, tarEndCache[tarfn]); err == nil {
			logger.Debug("end of %s: %d", tarfn, pos)
			tarEndCache[tarfn] = pos
		} else {
			logger.Error("error %s: %s", tarfn, err)
		}
	} else {
		if p, err = fh.Seek(0, 0); err != nil {
			logger.Error("error: %s: %s", tarfn, err)
		}
		pos = uint64(p)
	}
	tw = tar.NewWriter(fh)
	if err == nil && tw == nil {
		logger.Critical("couldn't open %+v!", tarfn)
		os.Exit(1)
	}
	fobj = fh
	logger.Debug("opened %s (err=%s): tw=%+v, fh=%+v, pos=%d",
		tarfn, err, tw, fh, pos)
	return
}

// create tar.Header from os.FileInfo
func Finfo2Theader(fi os.FileInfo) (hdr *tar.Header, err error) {
	m := fi.Mode()
	var (
		ln string
		tm byte
	)
	tm = tar.TypeReg
	switch {
	case m&os.ModeSymlink != 0:
		tm = tar.TypeSymlink
		/*if lfi, err := os.Lstat(fi.Name()); err == nil {
			ln = lfi.Name()
		}*/
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
	return
}

// tar.Header for a file
func FhandleTarHeader(fh *os.File) (hdr *tar.Header, err error) {
	if fi, err := fh.Stat(); err == nil {
		return Finfo2Theader(fi)
	}
	return
}

// tar.Header for a filename
func FileTarHeader(fn string) (hdr *tar.Header, err error) {
	if fi, err := os.Stat(fn); err == nil {
		return Finfo2Theader(fi)
	}
	return
}

// fills tar.Header missing information (uid/gid, username/groupname, times ...)
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

func WriteTar(tw *tar.Writer, hdr *tar.Header, r io.Reader) (err error) {
	if err = tw.WriteHeader(hdr); err != nil {
		logger.Error("error writing tar header %+v into %+v: %s", hdr, tw, err)
	}
	if hdr.Typeflag == tar.TypeSymlink {
	} else {
		_, err = io.Copy(tw, r)
	}
	if err != nil {
		logger.Critical("error copying tar data %+v into %+v: %s", r, tw, err)
		return
	}
	tw.Flush()
	return
}

// writes the Info to the tar
func writeInfo(tw *tar.Writer, info Info) (err error) {
	//logger.Printf("writeInfo")
	txt, length := info.NewReader()
	//logger.Printf("txt=%s", txt)
	hdr := &tar.Header{Name: info.Get(InfoPref+"Id") + SuffInfo, Mode: 0440,
		Size: int64(length), Typeflag: tar.TypeReg}
	FillHeader(hdr)
	logger.Debug("writeInfo(%+v into %+v)", hdr, tw)
	return WriteTar(tw, hdr, txt)
}

// writes compressed file to the tar
func writeCompressed(tw *tar.Writer, fn string, info Info,
	compressMethod string) (err error) {
	if sfh, err := os.Open(fn); err == nil {
		defer sfh.Close()
		if cfn, err := compressor.CompressToTemp(sfh, compressMethod); err == nil {
			logger.Debug("compressed file: %s", cfn)
			defer os.Remove(cfn)
			if cfh, err := os.Open(cfn); err == nil {
				if fi, err := cfh.Stat(); err == nil {
					defer cfh.Close()
					end := compressor.ShorterMethod(compressMethod)
					if hdr, err := FileTarHeader(fn); err == nil {
						hdr.Name = info.Get(InfoPref+"Id") + SuffData + end
						hdr.Size = fi.Size()
						hdr.Mode = 0400
						err = WriteTar(tw, hdr, cfh)
					}
				}
			}
		}
	}
	if err != nil {
		logger.Critical("couldn't write %s into %s: %s", fn, tw, err)
	}
	return
}

// appends file fn with info to tarfn, compressing with compressMethod
func AppendFile(tarfn string, info Info, fn string, compressMethod string) (pos uint64, err error) {
	tw, fh, pos, err := OpenForAppend(tarfn)
	if err != nil {
		logger.Error("OpenForAppend(%s): %s", tarfn, err)
		return
	}
	defer fh.Close()
	defer tw.Close()
	defer tw.Flush()
	if fi, err := os.Stat(fn); err == nil {
		info.Add(InfoPref+"Original-Size", fmt.Sprintf("%d", fi.Size()))
		info.Add(InfoPref+"Original-Name", fn)

		//pos, _ := fh.Seek(0, 1); logger.Printf("B %+v pos: %d", fh, pos)
		if err = writeInfo(tw, info); err == nil {
			// pos, _ = fh.Seek(0, 1); logger.Printf("C %+v pos: %d", fh, pos)
			err = writeCompressed(tw, fn, info, compressMethod)
			// pos, _ = fh.Seek(0, 1); logger.Printf("D %+v pos: %d", fh, pos)
		}
	}
	if err != nil {
		logger.Error("AppendFile: %s", err)
	} else {
		// pos, _ := fh.Seek(0, 1); logger.Printf("E %+v pos: %d", fh, pos)
		// tw.Flush()
		// pos, _ = fh.Seek(0, 1); logger.Printf("F %+v pos: %d", fh, pos)
		// tw.Close()
		// pos, _ = fh.Seek(0, 1); logger.Printf("G %+v pos: %d", fh, pos)
	}
	return
}

// appends as link pointing at a previously written item
func AppendLink(tarfn string, info Info, src string, dst string) (err error) {
	//var (twp *tar.Writer; fh ReadWriteSeekCloser; pos uint64)
	if tw, fh, _, err := OpenForAppend(tarfn); err == nil {
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
