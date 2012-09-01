// Copyright 2012 Tamás Gulácsi, UNO-SOFT Computing Ltd.
// This file is part of aostor.

// Aostor is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Aostor is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Aostor.  If not, see <http://www.gnu.org/licenses/>.

package compressor

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"fmt"
	"errors"
	"crypto/rand"
	"log"
	"compress/gzip"
	"compress/flate"
	)

var logger = log.New(bufio.NewWriter(os.Stderr), "compressor ", log.LstdFlags|log.Lshortfile)

// compresses from reader to a tempfile, returning its name
func CompressToTemp(r io.Reader, compressMethod string) (tempfn string, err error) {
	tempfn = os.TempDir() + fmt.Sprintf("/tarhelper-%s-%d.gz", RandString(8),
		os.Getpid())
	if fh, err := os.OpenFile(tempfn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600); err == nil {
		defer fh.Close()
		// logger.Printf("fh=%v", fh)
		if _, err := CompressCopy(fh, r, compressMethod); err != nil {
			//logger.Printf("copied %d bytes from %v to %v", n, f, gw)
			//} else {
			logger.Printf("copy from %v to %v error: %s", r, fh, err)
		}
	}
	return
}

// RandString returns a random string
func RandString(length int) string {
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		for i := 0; i < length; i++ {
			buf[i] = 0
		}
	}
	return fmt.Sprintf("%x", buf)
}

// CompressCopy copies from reader to writer, compressing in between using
// the given compressMethod
func CompressCopy(w io.Writer, r io.Reader, compressMethod string) (int64, error) {
	if compressMethod == "gzip" || compressMethod == "gz" {
		gw, err := gzip.NewWriterLevel(w, flate.BestCompression)
		if err != nil {
			return 0, err
		}
		defer gw.Close()
		// logger.Printf("CompressCopy copying from %s to %s", r, gw)
		return io.Copy(gw, r)
	} else {
		if compressMethod == "bz2" {
			compressMethod = "bzip2"
		}
		if _, ok := CompressorRegistry[compressMethod]; !ok {
			errors.New("unknown compress method " + compressMethod)
		}
		return 0, ExternalCompressCopy(w, r, compressMethod)
	}
	return 0, nil
}

// compressmethod -> compressor function map
var CompressorRegistry = make(map[string]string, 3)

func init() {
	for _, nm := range([]string{"bzip2", "gzip", "xz"}) {
		if path, err := exec.LookPath(nm); err == nil {
			CompressorRegistry[nm] = path
		}
	}
}

// uses external program for compression
func ExternalCompressCopy(dst io.Writer, src io.Reader, compressMethod string) error {
	prg, ok := CompressorRegistry[compressMethod]
	if !ok {
		return errors.New("unknown compress method " + compressMethod)
	}
	// logger.Printf("%s -9c - [%s -> %s]", prg, src, dst)
	cmd := exec.Command(prg, "-9c", "-")
	cmd.Stdin = src
	cmd.Stdout = dst
	var err error
	if err = cmd.Start(); err != nil {
		return err
	}
	if err = cmd.Wait(); err != nil {
		logger.Fatalf("compressor %s error: %s", cmd, err)
	}
	return err
}

// uses external program for decompression
func ExternalDecompressCopy(dst io.Writer, src io.Reader, compressMethod string) error {
	prg, ok := CompressorRegistry[compressMethod]
	if !ok {
		return errors.New("unknown compress method " + compressMethod)
	}
	// logger.Printf("%s -dc - [%s -> %s]", prg, src, dst)
	cmd := exec.Command(prg, "-dc", "-")
	cmd.Stdin = src
	cmd.Stdout = dst
	var err error
	if err = cmd.Start(); err != nil {
		return err
	}
	if err = cmd.Wait(); err != nil {
		logger.Fatalf("decompressor %s error: %s", cmd, err)
	}
	return err
}

// shortens the method name (gzip->gz, bzip2->bz2)
func ShorterMethod(name string) string {
	switch(name) {
	case "bzip2":
		return "bz2"
	case "gzip":
		return "gz"
	}
	return name
}