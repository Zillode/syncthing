// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package db provides a set type to track local/remote files with newness
// checks. We must do a certain amount of normalization in here. We will get
// fed paths with either native or wire-format separators and encodings
// depending on who calls us. We transform paths to wire-format (NFC and
// slashes) on the way to the database, and transform to native format
// (varying separator and encoding) on the way back out.
package db

import (
	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/sync"
)

type FileSet struct {
	mutex    sync.RWMutex
	folder   string
	db       *SQLiteDB
	blockmap *BlockMap
}

// FileIntf is the set of methods implemented by both protocol.FileInfo and
// protocol.FileInfoTruncated.
type FileIntf interface {
	Size() int64
	IsDeleted() bool
	IsInvalid() bool
	IsDirectory() bool
	IsSymlink() bool
	HasPermissionBits() bool
}

// The Iterator is called with either a protocol.FileInfo or a
// protocol.FileInfoTruncated (depending on the method) and returns true to
// continue iteration, false to stop.
type Iterator func(f FileIntf) bool

func NewFileSet(folder string, db *SQLiteDB) *FileSet {
	var s = FileSet{
		folder: folder,
		db:     db,
		//blockmap: NewBlockMap(db, folder), // TODO
		mutex: sync.NewRWMutex(),
	}

	clock(s.db.maxID(folder, protocol.LocalDeviceID))

	return &s
}

func (s *FileSet) Replace(device protocol.DeviceID, fs []protocol.FileInfo) {
	if debug {
		l.Debugf("%s Replace(%v, [%d])", s.folder, device, len(fs))
	}
	normalizeFilenames(fs)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.db.replace(s.folder, device, fs)
	if err != nil {
		panic(err)
	}
}

func (s *FileSet) Update(device protocol.DeviceID, fs []protocol.FileInfo) {
	if debug {
		l.Debugf("%s Update(%v, [%d])", s.folder, device, len(fs))
	}
	normalizeFilenames(fs)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := s.db.update(s.folder, device, fs)
	if err != nil {
		panic(err)
	}
}

func (s *FileSet) WithNeed(device protocol.DeviceID, fn Iterator) {
	if debug {
		l.Debugf("%s WithNeed(%v)", s.folder, device)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.need(s.folder, device, nativeFileIterator(fn))
}

func (s *FileSet) WithNeedTruncated(device protocol.DeviceID, fn Iterator) {
	if debug {
		l.Debugf("%s WithNeedTruncated(%v)", s.folder, device)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.needTruncated(s.folder, device, nativeFileIterator(fn))
}

func (s *FileSet) WithHave(device protocol.DeviceID, fn Iterator) {
	if debug {
		l.Debugf("%s WithHave(%v)", s.folder, device)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.have(s.folder, device, nativeFileIterator(fn))
}

func (s *FileSet) WithHaveTruncated(device protocol.DeviceID, fn Iterator) {
	if debug {
		l.Debugf("%s WithHaveTruncated(%v)", s.folder, device)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.haveTruncated(s.folder, device, nativeFileIterator(fn))
}

func (s *FileSet) WithGlobal(fn Iterator) {
	if debug {
		l.Debugf("%s WithGlobal()", s.folder)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.global(s.folder, nativeFileIterator(fn))
}

func (s *FileSet) WithGlobalTruncated(fn Iterator) {
	if debug {
		l.Debugf("%s WithGlobalTruncated()", s.folder)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.globalTruncated(s.folder, nativeFileIterator(fn))
}

func (s *FileSet) WithPrefixedGlobalTruncated(prefix string, fn Iterator) {
	if debug {
		l.Debugf("%s WithPrefixedGlobalTruncated()", s.folder, prefix)
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.db.prefixedGlobalTruncated(s.folder, osutil.NormalizedFilename(prefix), nativeFileIterator(fn))
}

func (s *FileSet) Get(device protocol.DeviceID, file string) (protocol.FileInfo, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	f, ok := s.db.get(s.folder, device, osutil.NormalizedFilename(file))
	f.Name = osutil.NativeFilename(f.Name)
	return f, ok
}

func (s *FileSet) GetGlobal(file string) (protocol.FileInfo, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	f, ok := s.db.getGlobal(s.folder, osutil.NormalizedFilename(file))
	f.Name = osutil.NativeFilename(f.Name)
	return f, ok
}

func (s *FileSet) GetGlobalTruncated(file string) (FileInfoTruncated, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	f, ok := s.db.getGlobalTruncated(s.folder, osutil.NormalizedFilename(file))
	f.Name = osutil.NativeFilename(f.Name)
	return f, ok
}

func (s *FileSet) Availability(file string) []protocol.DeviceID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.db.availability(s.folder, file)
}

func (s *FileSet) LocalVersion(device protocol.DeviceID) int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.db.maxID(s.folder, device)
}

// TODO
//// ListFolders returns the folder IDs seen in the database.
//func ListFolders(db *leveldb.DB) []string {
//	return ldbListFolders(db)
//}
//
//// DropFolder clears out all information related to the given folder from the
//// database.
//func DropFolder(db *leveldb.DB, folder string) {
//	ldbDropFolder(db, []byte(folder))
//	bm := &BlockMap{
//		db:     db,
//		folder: folder,
//	}
//	bm.Drop()
//	NewVirtualMtimeRepo(db, folder).Drop()
//}

func normalizeFilenames(fs []protocol.FileInfo) {
	for i := range fs {
		fs[i].Name = osutil.NormalizedFilename(fs[i].Name)
	}
}

func nativeFileIterator(fn Iterator) Iterator {
	return func(fi FileIntf) bool {
		switch f := fi.(type) {
		case protocol.FileInfo:
			f.Name = osutil.NativeFilename(f.Name)
			return fn(f)
		case FileInfoTruncated:
			f.Name = osutil.NativeFilename(f.Name)
			return fn(f)
		default:
			panic("unknown interface type")
		}
	}
}
