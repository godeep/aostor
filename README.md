# aostor

Append-Only File Storage
Stores files in an append-only manner, indexed for fast retrieval, but uses few files to be easy on fs (have you ever tried to list all files in a 3 million files directory hierarchy? It takes ages!)

## Problem
Lots of small files, slow fs over 1000 files/dir. Possible archiving after a long period of time - for restore, a compact solution needed.

## A possible solution
Store files in tars (say, 1Gb each), right with the metadata, too.

## Tar layout
Each stored file gets a unique key (UUID), data is stored as <key>#<compress_method>, i.e. 213f34a8dc1d213f34a8dc1d213f34a8#bz2.
The metadata (info) is stored in <key>!, the possible symbolic link (for per-tar deduping) is as <key>@.

The info is in HTTP header format ("\n" separated lines, ": " separated key and value), each aostor-specific header (id, index position (ipos) and data position (dpos)) starting with X-Aostor-.

### Indexing
Tar needs an index, to be able retrieve files in random order. For this, each tar gets a .cdb companion (D. J. Bernstein's Constant DataBase).

TODO: one needs to find out in which tar the file is in!

## Appending files
Files written into a simple directory ("staging"), just as they would be in the tar. If the count/size reaches a threshold, they're shoveled in a tar, accompanied by the .cdb.


## Retrieving a file
First the staging directory is checked, if the <key>! (info) file is there, then read, and the <key>#bz2 is checked.

If the staging directory is empty, then we start searching the cdbs, first the newest (L0), then the next level (L1), then the next (L2), and so on.


## Index "compaction"
When *shovel* is called, the files in the staging dir are shoveled in some tars, accompanied by .cdb. The .cdb is symlinked into the L0 directory.
Then the L1 directory is checked: if then number of cdbs are bigger than the threshold (10), then they are merged into a new cdb in the L1 directory, and these L0 cdbs are deleted.
If this happened, then the L(n+1) dir is checked: if the number of cdbs are bigger than the threshold (10), then they are merged into a new cdb in the L(n+2) directory, and these L(n+1) cdbs are deleted.

CDB has a size limit of 2Gb, so the compactor must take this into account, too!


API Docs: http://go.pkgdoc.org/github.com/tgulacsi/aostor