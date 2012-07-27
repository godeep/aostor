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

const MAX_CDB_SIZE = (1 << 31) - 1

//Compact compacts the index cdbs
func CompactIndices(realm string, level uint) error {
	conf, err := ReadConf("", realm)
	if err != nil {
		return err
	}
	var n int
	for ; level < 10; level++ {
		n, err = compactLevel(level, conf.IndexDir, conf.IndexThreshold)
		if err != nil {
			return err
		} else if n == 0 {
			break
		}
	}
	return err
}

func strNow() string {
	return strings.Replace(strings.Replace(time.Now().Format(time.RFC3339), "-", "", -1), ":", "", -1)
}

func compactLevel(level uint, index_dir string, threshold uint) (int, error) {
	num := 0
	path := index_dir + "/" + fmt.Sprintf("L%02d", level)
	files_a, err := filepath.Glob(path + "/*.cdb")
	if err != nil {
		return 0, err
	}
	length := uint(len(files_a))
	if length < threshold {
		return 0, nil
	}
	files := make(sizedFilenames, length)
	j := 0
	for _, fn := range files_a {
		fsize := fileSize(fn)
		if fsize > 0 {
			files[j] = sizedFilename{fn, fsize}
			j++
		}
	}
	sort.Sort(bySizeReversed{files})
	dest_dir := index_dir + "/" + fmt.Sprintf("L%02d", level+1)
	lskip := 0
	for lskip < len(files) {
		fbuf := make([]string, threshold)
		j := 0
		size := int64(0)
		askip := 0
		for i, sizedfn := range files[lskip:] {
			if size+sizedfn.size < MAX_CDB_SIZE {
				fbuf[j] = sizedfn.filename
				j++
				size += sizedfn.size
				//delete(files, i)
				if uint(len(fbuf)) >= threshold {
					break
				}
			} else {
				if askip == 0 {
					askip = i
				}
			}
		}
		if askip == 0 {
			askip = len(fbuf)
		}
		lskip += askip

		uuid, err := StrUUID()
		if err != nil {
			return 0, err
		}
		err = mergeCdbs(dest_dir+"/"+strNow()+"-"+uuid+".cdb", fbuf,
			level, threshold, true)
		if err != nil {
			return 0, err
		}
		num += len(fbuf)
	}
	return num, err
}

type sizedFilename struct {
	filename string
	size     int64
}
type sizedFilenames []sizedFilename

func (s sizedFilenames) Len() int           { return len(s) }
func (s sizedFilenames) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sizedFilenames) Less(i, j int) bool { return s[i].size < s[j].size }

type bySizeReversed struct{ sizedFilenames }

func (s bySizeReversed) Less(i, j int) bool {
	return s.sizedFilenames[i].size > s.sizedFilenames[j].size
}

func mergeCdbs(dest_cdb_fn string, source_cdb_files []string, level uint, threshold uint, move bool) error {
	cw, err := cdb.NewWriter(dest_cdb_fn)
	if err != nil {
		return err
	}
	booknum := 0
	var book_id []byte
	var books map[string]string
	for _, sfn := range source_cdb_files {
		if sfn == "" {
			continue
		}
		if level == 0 {
			book_id = StrToBytes(fmt.Sprintf("/%d", booknum))
			booknum++
			//FIXME: store only the relative path?
			logger.Printf("sfn=%s", sfn)
			logger.Printf("sfn=%s", sfn[:len(sfn)-4])
			cw.PutPair(book_id, StrToBytes(sfn[:len(sfn)-4]))
		} else {
			books = make(map[string]string, threshold<<(3*level))
		}
		sfh, err := os.Open(sfn)
		if err != nil {
			return err
		}
		cr := make(chan cdb.Element, 1)
		go cdb.DumpToChan(cr, sfh)
		for {
			elt, ok := <-cr
			if !ok {
				break
			}
			if level == 0 {
				cw.PutPair(elt.Key, book_id)
			} else {
				if elt.Key[0] == '/' {
					bs := fmt.Sprintf("/%d", booknum)
					books[BytesToStr(elt.Key)] = bs
					book_id = StrToBytes(bs)
					booknum++
					cw.PutPair(book_id, elt.Data)
				} else {
					if _, ok := books[BytesToStr(elt.Data)]; !ok {
						logger.Panicf("unknown book %s", elt.Data)
					}
					cw.PutPair(elt.Key, StrToBytes(books[BytesToStr(elt.Data)]))
				}
			}
		}
		sfh.Close()
	}
	cw.Close()
	if !fileExists(dest_cdb_fn) {
		return errors.New("cdb " + dest_cdb_fn + " not exists!")
	}
	if move {
		for _, fn := range source_cdb_files {
			if fn == "" {
				continue
			}
			err = os.Remove(fn)
			if err != nil {
				return err
			}
		}
	}

	return err
}

//Returns the size of the file, -1 on error
func fileSize(fn string) int64 {
	if fh, err := os.Open(fn); err == nil {
		if fi, err := fh.Stat(); err == nil {
			return fi.Size()
		}
	}
	return -1
}
