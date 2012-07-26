package aostor

import (
	"fmt"
	//"io"
	"os"
	"time"
	"strings"
	"path/filepath"
	"github.com/tgulacsi/go-cdb"
	)

//Compact compacts the index cdbs
func CompactIndices(level int) error {
	conf, err := ReadConf("")
	if err != nil {
		return err
	}
	index_dir, err := conf.GetString("dirs", "index")
	if err != nil {
		return err
	}
	threshold, err := conf.GetInt("threshold", "index")
	if err != nil {
		threshold = 10
	}
	var n int
	for level := 1; level < 10; level++ {
		n, err = compactLevel(level, index_dir, threshold)
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

func compactLevel(level int, index_dir string, threshold int) (int, error) {
	num := 0
	path := index_dir + "/" + fmt.Sprintf("L%02d", level)
	files, err := filepath.Glob(path + "/*.cdb")
	if err != nil {
		return 0, err
	}
	if len(files) < threshold {
		return 0, nil
	}
	dest_dir := index_dir + "/" + fmt.Sprintf("L%02d", level+1)
	for i := 1; i *threshold <= len(files); i++ {
		err = mergeCdbs(dest_dir + "/" + strNow() + ".cdb",
			files[(i-1)*threshold:i*threshold], level)
		if err != nil {
			return 0, err
		}
		num += threshold
	}
	return num, err
}

func mergeCdbs(dest_cdb_fn string, source_cdb_files []string, level int) error {
	cw, err := cdb.NewWriter(dest_cdb_fn)
	defer cw.Close()
	if err != nil {
		return err
	}
	booknum := 0
	var book_id []byte
	var books map[string]string
	for _, sfn := range(source_cdb_files) {
		if level == 0 {
			book_id = StrToBytes(fmt.Sprintf("/%d", booknum))
			booknum++
			//FIXME: store only the relative path?
			cw.PutPair(book_id, StrToBytes(sfn[:-4]))
		} else {
			books = make(map[string]string, 10 ^ level)
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

	return err
}
