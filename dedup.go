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
	"github.com/tgulacsi/go-locking"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// deduplication: replace data with a symlink to a previous data with the same contant-hash-...
func DeDup(path string, hash string, alreadyLocked bool) int {
	var err error
	if !alreadyLocked {
		if locks, err := locking.FLockDirs(path); err != nil {
			logger.Errorf("cannot create lock for %s: %s", path, err)
			return -1
		} else {
			defer locking.FLocks(locks).Unlock()
		}
	}
	n := 0
	//contentHash -> elements map
	hashes := make(map[string][]fElt, 16)
	//contentHash -> already existing symlinks' original target
	primals := make(map[string]string, 16)
	var hamster listDirFunc = func(elt fElt) error {
		if debug2 {
			logger.Debugf("%s sl? %s lo=%s", elt.contentHash, elt.isSymlink, FindLinkOrigin(elt.dataFn, false))
		}
		if elt.contentHash == "" {
			return nil
		}
		//only one primal should exist!
		if elt.isSymlink {
			if prim, ok := primals[elt.contentHash]; ok {
				logger.Tracef("prim=%s orig=%s", prim, elt.dataFnOrig)
				if same, e := SameFile(elt.dataFnOrig, prim); e != nil {
					logger.Errorf("cannot check equivalence of %s and %s: %s", elt.dataFnOrig, prim, e)
					if os.IsNotExist(e) {
						os.Exit(2)
					}
				} else if !same {
					logger.Warnf("already exists differend origin (%s) for %s!",
						prim, elt.dataFnOrig)
					if err = os.Remove(elt.dataFn); err != nil {
						logger.Errorf("cannot remove %s: %s", elt.dataFn, err)
					}
					destfn := CalculateLink(filepath.Dir(elt.dataFn), prim)
					if err = os.Symlink(destfn, elt.dataFn); err != nil {
						logger.Errorf("cannot create symlink %s for %s", elt.dataFn, destfn)
						os.Exit(3)
					}
				}
			} else {
				primals[elt.contentHash] = elt.dataFnOrig
			}
		}
		if other, ok := hashes[elt.contentHash]; ok {
			hashes[elt.contentHash] = append(other, elt)
		} else {
			hashes[elt.contentHash] = []fElt{elt}
		}
		return nil
	}
	logger.Debug("primals=", primals)

	if err = listDirMap(path, hash, hamster); err != nil {
		logger.Errorf("error listing %s: %s", path, err)
		return 0
	}
	var (
		p    int
		prim string
	)
	for contentHash, elts := range hashes {
		prim = primals[contentHash]
		for _, elt := range elts {
			if prim == "" {
				if elt.isSymlink {
					prim = elt.dataFnOrig
				} else {
					prim = elt.dataFn
				}
				continue
			}
			if elt.isSymlink {
				continue
			}
			if prim == elt.dataFn {
				// logger.Info("skipping symlink origin: ", prim)
				continue
			}
			if err := os.Remove(elt.dataFn); err != nil {
				logger.Warnf("cannot remove %s: %s", elt.dataFn, err)
				continue
			} else {
				destfn := CalculateLink(filepath.Dir(elt.dataFn), prim)
				linkfn := elt.dataFn
				p = len(linkfn) - 1
				if linkfn[p:p+len(SuffData)] == SuffData || linkfn[p:p+len(SuffLink)] == SuffLink {
				} else {
					p = strings.LastIndex(linkfn, SuffData)
					if p < 0 {
						p = strings.LastIndex(linkfn, SuffLink)
					}
				}
				linkfn = linkfn[:p] + SuffLink
				logger.Debugf("creating symlink from %s to %s", linkfn, destfn)
				if err := os.Symlink(destfn, linkfn); err != nil {
					logger.Warnf("cannot create symlink %s for %s: %s",
						linkfn, destfn, err)
					os.Exit(4)
				} else {
					n++
				}
			}
		}
	}
	return n
}

// CalculateLink calculates the symbolic link for destfn relative to basedir
func CalculateLink(basedir, destfn string) string {
	if basedir == filepath.Dir(destfn) {
		return filepath.Base(destfn)
	}
	var err error
	if destfn, err = filepath.Rel(basedir, destfn); err != nil {
		logger.Warn("no rel path for destfn: ", err)
	}
	return destfn
}

func SameFile(fn1, fn2 string) (bool, error) {
	if fn1 == fn2 {
		return true, nil
	}
	fi1, err := os.Stat(fn1)
	if err != nil {
		logger.Errorf("error stating %s: %s", fn1, err)
		return false, err
	}
	fi2, err := os.Stat(fn2)
	if err != nil {
		logger.Errorf("error stating %s: %s", fn2, err)
		return false, err
	}
	return os.SameFile(fi1, fi2), nil
}

type fElt struct {
	info   Info
	infoFn string
	dataFn string
	// contentHash []byte
	contentHash string
	isSymlink   bool
	dataFnOrig  string
}

type listDirFunc func(fElt) error

var debug2 bool = false

func listDirMap(path string, hash string, hamster listDirFunc) error {
	if debug2 {
		logger.Debug("listDirMap " + path)
	}
	possibleEndings := []string{SuffData, SuffLink}
	var (
		info, emptyInfo Info
		elt, emptyElt   fElt
	)
	emptyElt.isSymlink = false
	buf := make([]fElt, 0)
	infobuf := make([]byte, 8192)
	var n int
	var err error

	var pedestrian filepath.WalkFunc = func(path string, fi os.FileInfo, err error) error {
		if debug2 {
			logger.Debug("pedestrian " + path + " " + fi.Name())
		}
		if fi.IsDir() {
			if debug2 {
				logger.Debugf("skipping dir %s", fi.Name())
			}
			return nil
		}
		if debug2 {
			logger.Debugf("path=%s fi=%s", path, fi.Name())
		}
		bn := fi.Name()
		if !strings.HasSuffix(bn, SuffInfo) {
			logger.Trace("skip ", bn)
			return nil
		}
		if !fileExists(path) {
			logger.Warn("not exists! ", path)
			return nil
		}
		info, elt = emptyInfo, emptyElt
		elt.infoFn = path
		// bn := BaseName(fn)
		info.Key, err = UUIDFromString(bn[:len(bn)-1])
		if err != nil {
			logger.Errorf("cannot treat %s as uuid: %s", bn[:len(bn)-1], err)
			return nil
		}
		if ifh, err := os.Open(elt.infoFn); err == nil {
			if fi, _ := ifh.Stat(); err == nil && fi.Size() <= int64(cap(infobuf)) {
				if n, err = io.ReadFull(ifh, infobuf[:fi.Size()]); err == nil {
					info, err = InfoFromBytes(infobuf[:n])
				}
			} else {
				info, err = ReadInfo(ifh)
			}
			_ = ifh.Close()
			if err != nil {
				logger.Errorf("cannot read info from %s: %s", elt.infoFn, err)
				return nil
			}
		} else {
			logger.Errorf("cannot read info from %s: %s", elt.infoFn, err)
			return nil
		}

		pref := path[:len(path)-len(SuffInfo)]
		if fileExists(pref + SuffLink) {
			elt.dataFn = pref + SuffLink
			elt.isSymlink = true
		} else {
			for _, end := range possibleEndings {
				logger.Tracef("checking %s: %s", pref+end, fileExists(pref+end))
				if fileExists(pref + end) {
					elt.dataFn = pref + end
					break
				}
			}
			if elt.dataFn == "" {
				logger.Warn("cannot find data file for ", elt.infoFn)
				return nil
			}
		}
		if debug2 {
			logger.Debugf("elt=%s", elt)
		}
		elt.info = info
		if hash != "" {
			elt.contentHash = info.Get(InfoPref + "Content-" + hash)
		}
		if elt.isSymlink {
			elt.dataFnOrig = FindLinkOrigin(elt.dataFn, true)
			if !filepath.IsAbs(elt.dataFnOrig) {
				elt.dataFnOrig = filepath.Clean(filepath.Join(path, elt.dataFnOrig))
			}
			buf = append(buf, elt)
		} else {
			if err = hamster(elt); err != nil {
				if err == StopIteration {
					return StopIteration
				} else {
					logger.Errorf("error with %s: %s", elt, err)
					return err
				}
			}
		}
		return nil
	}

	logger.Tracef("calling Walk(%s)", path)
	if err = Walk(path, pedestrian); err != nil {
		logger.Errorf("error walking %s: %s", path, err)
		return err
	}

	for _, elt := range buf {
		if err = hamster(elt); err != nil {
			if err == StopIteration {
				break
			} else {
				logger.Errorf("error with %s: %s", elt, err)
				return err
			}
		}
	}
	return nil
}

func walk(path string, info os.FileInfo, walkFn filepath.WalkFunc) error {
	// logger.Tracef("walk(%s, %s)", path, info.Name())
	err := walkFn(path, info, nil)
	if err != nil {
		if err == StopIteration {
			return nil
		}
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	dh, err := os.Open(path)
	if err != nil {
		return walkFn(path, info, err)
	}
	defer dh.Close()
	for {
		list, err := dh.Readdir(1024)
		if err != nil {
			if err == io.EOF {
				break
			}
			return walkFn(path, info, err)
		}

		for _, fileInfo := range list {
			err = walk(filepath.Join(path, fileInfo.Name()), fileInfo, walkFn)
			if err != nil {
				if err == StopIteration {
					return nil
				}
				if !fileInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}

	return nil
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. All errors that arise visiting files
// and directories are filtered by walkFn. The files are walked in inode
// order, which makes the output indeterministic but means that even for very
// large directories Walk can be efficient.
func Walk(root string, walkFn filepath.WalkFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		return walkFn(root, nil, err)
	}
	return walk(root, info, walkFn)
}
