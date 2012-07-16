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
Files written into a simple directory, just as they would be in the tar. If the count/size reaches a threshold, they're shoveled in a tar, accompanied by the .cdb.