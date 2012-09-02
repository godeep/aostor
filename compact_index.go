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
	"fmt"
	//"io"
	"errors"
	"github.com/tgulacsi/go-cdb"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	MIN_CDB_SIZE = 2048
	MAX_CDB_SIZE = (1 << 31) - 1
)

var checkMerge bool = false

//Compact compacts the index cdbs
func CompactIndices(realm string, level uint, onChange func()) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	var n int
	for ; level < 10; level++ {
		n, err = compactLevel(level, conf.IndexDir, conf.IndexThreshold)
		if err != nil {
			logger.Error("compactLevel(%s, %s, %s): %s", level, conf.IndexDir, conf.IndexThreshold, err)
			return err
		} else if n == 0 {
			break
		}
	}
	return nil
}

func strNow() string {
	return strings.Replace(strings.Replace(time.Now().Format(time.RFC3339), "-", "", -1), ":", "", -1)
}

func compactLevel(level uint, index_dir string, threshold uint) (int, error) {
	num := 0
	path := index_dir + "/" + fmt.Sprintf("L%02d", level)
	files_a, err := filepath.Glob(path + "/*.cdb")
	if err != nil {
		logger.Error("cannot list files in %s: %s", path, err)
		return 0, err
	}
	files := make(sizedFilenames, 0, 1024)
	for _, fn := range files_a {
		if fn == "" {
			continue
		}
		fsize := fileSize(fn)
		if fsize > MIN_CDB_SIZE {
			files = append(files, &sizedFilename{fn, fsize})
		}
	}
	length := uint(len(files))
	if length <= threshold {
		return 0, nil
	}
	sort.Sort(bySizeReversed{files})
	dest_dir := index_dir + "/" + fmt.Sprintf("L%02d", level+1)
	lskip := uint(0)
	for lskip < length {
		fbuf := make([]string, threshold)
		j := 0
		size := int64(0)
		askip := uint(0)
		for i, sizedfn := range files[lskip:] {
			if size+sizedfn.size < MAX_CDB_SIZE {
				// if !fileExists(sizedfn.filename) {
				// 	logger.Warn("compactLevel: %s not exists!", sizedfn.filename)
				// 	continue
				// }
				fbuf[j] = sizedfn.filename
				j++
				size += sizedfn.size
				//delete(files, i)
				if uint(j) >= threshold {
					fbuf = fbuf[:j]
					break
				}
			} else {
				if askip == 0 {
					askip = uint(i)
				}
			}
		}
		if askip == 0 {
			askip = uint(len(fbuf))
		}
		lskip += askip

		uuid, err := NewUUID()
		if err != nil {
			logger.Critical("cannot generate uuid: %s", err)
			return 0, err
		}
		dest_cdb_fn := dest_dir + "/" + strNow()[:15] + "-" + uuid.String() + ".cdb"
		err = mergeCdbs(dest_cdb_fn, fbuf, level, threshold, true)
		if err != nil {
			logger.Error("mergeCdbs(%s, %s, %s, %s, %s): %s", dest_cdb_fn, fbuf, level, threshold, true, err)
			return 0, err
		}
		num += len(fbuf)
	}
	return num, nil
}

type sizedFilename struct {
	filename string
	size     int64
}
type sizedFilenames []*sizedFilename

func (s sizedFilenames) Len() int           { return len(s) }
func (s sizedFilenames) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sizedFilenames) Less(i, j int) bool { return s[i].size < s[j].size }

type bySizeReversed struct{ sizedFilenames }

func (s bySizeReversed) Less(i, j int) bool {
	fmt.Sprintf("i=%d, j=%d, len=%d", i, j, len(s.sizedFilenames))
	return s.sizedFilenames[i].size > s.sizedFilenames[j].size
}

//merges cdbs
///%02d is a book id, which exists as key and value, too.
//The key's value is the tar file's name
func mergeCdbs(dest_cdb_fn string, source_cdb_files []string, level uint, threshold uint, move bool) error {
	if uint(len(source_cdb_files)) < threshold {
		return nil
	}
	dn := filepath.Dir(dest_cdb_fn)
	if !fileExists(dn) {
		if err := os.MkdirAll(dn, 0755); err != nil {
			logger.Error("cannot create dest directory %s: %s", dn, err)
			return err
		}
	}
	cw, err := cdb.NewWriter(dest_cdb_fn)
	if err != nil {
		logger.Error("cannot open dest cdb %s: %s", dest_cdb_fn, err)
		return err
	}
	booknum := 0
	var book_id []byte
	var books map[string]string
	var check map[string]string
	var lengths map[string]int
	if checkMerge {
		check = make(map[string]string, 1024)
		lengths = make(map[string]int, 10)
	}
	tbd := make([]string, 0)
	for _, sfn := range source_cdb_files {
		if sfn == "" {
			// logger.Warn("mergeCdbs: sfn=%s not exists!", sfn)
			continue
		}
		n := 0
		if level == 0 {
			book_id = StrToBytes(fmt.Sprintf("/%d", booknum))
			if book_id[0] != '/' {
				logger.Critical("book_id=%s not starts with /??", book_id)
				os.Exit(1)
			}
			booknum++
			//FIXME: store only the relative path?
			logger.Debug("put(%s,%s)", book_id, sfn)
			// logger.Debug("sfn=%s [%d]", sfn, len(sfn))
			// logger.Debug("1=%s", sfn[:len(sfn)-4])
			// logger.Debug("2=%s", StrToBytes(sfn[:len(sfn)-4]))
			cw.PutPair(book_id, StrToBytes(filepath.Base(sfn[:len(sfn)-4])))
		} else {
			books = make(map[string]string, threshold<<(3*level))
		}
		sfh, err := os.Open(sfn)
		if err != nil {
			logger.Error("cannot open source cdb %s: %s", sfn, err)
			return err
		}
		cr := make(chan cdb.Element, 1)
		go cdb.DumpToChan(cr, sfh)
		logger.Debug("Dumping %s into %s", sfn, dest_cdb_fn)
		for {
			elt, ok := <-cr
			if !ok {
				break
			}
			logger.Trace("elt=%s", elt)
			if level == 0 {
				logger.Trace("put(%s,%s)", elt.Key, book_id)
				cw.PutPair(elt.Key, book_id)
				if checkMerge {
					check[BytesToStr(elt.Key)] = sfn
				}
				n++
			} else {
				if elt.Key[0] == '/' {
					bs := fmt.Sprintf("/%d", booknum)
					books[BytesToStr(elt.Key)] = bs
					book_id = StrToBytes(bs)
					booknum++
					logger.Debug("put(%s,%s)", book_id, elt.Data)
					cw.PutPair(book_id, elt.Data)
				} else {
					if _, ok := books[BytesToStr(elt.Data)]; !ok {
						logger.Critical("level %d, unknown book %s of %s from %s (known: %+v)",
							level, elt.Data, elt.Key, sfh.Name(), books)
						os.Exit(1)
					}
					cw.PutPair(elt.Key, StrToBytes(books[BytesToStr(elt.Data)]))
					if checkMerge {
						check[BytesToStr(elt.Key)] = sfn
					}
					n++
				}
			}
		}
		sfh.Close()
		if move {
			tbd = append(tbd, sfn)
		}
		if checkMerge {
			lengths[sfn] = n
		}
	}
	cw.Close()
	if !fileExists(dest_cdb_fn) {
		return errors.New("cdb " + dest_cdb_fn + " not exists!")
	}
	if checkMerge {
		fh, err := os.Open(dest_cdb_fn)
		if err != nil {
			logger.Critical("cannot open %s", dest_cdb_fn)
			os.Exit(1)
		}
		cr := make(chan cdb.Element, 1)
		go cdb.DumpToChan(cr, fh)
		n := 0
		for {
			elt, ok := <-cr
			if !ok {
				break
			}
			if elt.Key[0] != '/' {
				n++
				k := BytesToStr(elt.Key)
				_, ok := check[k]
				if ok {
					delete(check, k)
				} else {
					logger.Critical("CheckMerge error: %s in merged db, but not in checklist", k)
					os.Exit(1)
				}
			}
		}
		length_sum := 0
		for _, i := range lengths {
			length_sum += i
		}
		logger.Info("merged dump: %d elts=%s sum=%d OK? %s", n, lengths,
			length_sum, length_sum == n)
		if len(check) > 0 {
			logger.Critical("CheckMerge error: checklist not empty: %s", check)
			os.Exit(1)
		}
	}
	if move {
		for _, fn := range tbd {
			logger.Info("deleting %s", fn)
			err = os.Remove(fn)
			if err != nil {
				logger.Error("cannot remove %s", fn)
				return err
			}
		}
	}

	return nil
}

//Returns the size of the file, -1 on error
func fileSize(fn string) int64 {
	if fh, err := os.Open(fn); err == nil {
		defer fh.Close()
		if fi, err := fh.Stat(); err == nil {
			return fi.Size()
		}
	}
	return -1
}
