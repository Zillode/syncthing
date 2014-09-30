// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// Package files provides a set type to track local/remote files with newness
// checks. We must do a certain amount of normalization in here. We will get
// fed paths with either native or wire-format separators and encodings
// depending on who calls us. We transform paths to wire-format (NFC and
// slashes) on the way to the database, and transform to native format
// (varying separator and encoding) on the way back out.
package files

import (
	"sync"

	"github.com/syncthing/syncthing/internal/lamport"
	"github.com/syncthing/syncthing/internal/protocol"
	"github.com/syndtr/goleveldb/leveldb"
)

type fileRecord struct {
	File   protocol.FileInfo
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	localVersion map[protocol.DeviceID]uint64
	mutex        sync.Mutex
	folder       string
	db           *leveldb.DB
}

func NewSet(folder string, db *leveldb.DB) *Set {
	var s = Set{
		localVersion: make(map[protocol.DeviceID]uint64),
		folder:       folder,
		db:           db,
	}

	var deviceID protocol.DeviceID
	ldbWithAllFolderTruncated(db, []byte(folder), func(device []byte, f protocol.FileInfoTruncated) bool {
		copy(deviceID[:], device)
		if f.LocalVersion > s.localVersion[deviceID] {
			s.localVersion[deviceID] = f.LocalVersion
		}
		lamport.Default.Tick(f.Version)
		return true
	})
	if debug {
		l.Debugf("loaded localVersion for %q: %#v", folder, s.localVersion)
	}
	clock(s.localVersion[protocol.LocalDeviceID])

	return &s
}

func (s *Set) Replace(device protocol.DeviceID, fs []protocol.FileInfo) {
	if debug {
		l.Debugf("%s Replace(%v, [%d])", s.folder, device, len(fs))
	}
	normalizeFilenames(fs)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.localVersion[device] = ldbReplace(s.db, []byte(s.folder), device[:], fs)
	if len(fs) == 0 {
		// Reset the local version if all files were removed.
		s.localVersion[device] = 0
	}
}

func (s *Set) ReplaceWithDelete(device protocol.DeviceID, fs []protocol.FileInfo) {
	if debug {
		l.Debugf("%s ReplaceWithDelete(%v, [%d])", s.folder, device, len(fs))
	}
	normalizeFilenames(fs)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if lv := ldbReplaceWithDelete(s.db, []byte(s.folder), device[:], fs); lv > s.localVersion[device] {
		s.localVersion[device] = lv
	}
}

func (s *Set) Update(device protocol.DeviceID, fs []protocol.FileInfo) {
	if debug {
		l.Debugf("%s Update(%v, [%d])", s.folder, device, len(fs))
	}
	normalizeFilenames(fs)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if lv := ldbUpdate(s.db, []byte(s.folder), device[:], fs); lv > s.localVersion[device] {
		s.localVersion[device] = lv
	}
}

func (s *Set) WithNeed(device protocol.DeviceID, fn fileIterator) {
	if debug {
		l.Debugf("%s WithNeed(%v)", s.folder, device)
	}
	ldbWithNeed(s.db, []byte(s.folder), device[:], false, nativeFileIterator(fn))
}

func (s *Set) WithNeedTruncated(device protocol.DeviceID, fn fileIterator) {
	if debug {
		l.Debugf("%s WithNeedTruncated(%v)", s.folder, device)
	}
	ldbWithNeed(s.db, []byte(s.folder), device[:], true, nativeFileIterator(fn))
}

func (s *Set) WithHave(device protocol.DeviceID, fn fileIterator) {
	if debug {
		l.Debugf("%s WithHave(%v)", s.folder, device)
	}
	ldbWithHave(s.db, []byte(s.folder), device[:], false, nativeFileIterator(fn))
}

func (s *Set) WithHaveTruncated(device protocol.DeviceID, fn fileIterator) {
	if debug {
		l.Debugf("%s WithHaveTruncated(%v)", s.folder, device)
	}
	ldbWithHave(s.db, []byte(s.folder), device[:], true, nativeFileIterator(fn))
}

func (s *Set) WithGlobal(fn fileIterator) {
	if debug {
		l.Debugf("%s WithGlobal()", s.folder)
	}
	ldbWithGlobal(s.db, []byte(s.folder), false, nativeFileIterator(fn))
}

func (s *Set) WithGlobalTruncated(fn fileIterator) {
	if debug {
		l.Debugf("%s WithGlobalTruncated()", s.folder)
	}
	ldbWithGlobal(s.db, []byte(s.folder), true, nativeFileIterator(fn))
}

func (s *Set) Get(device protocol.DeviceID, file string) protocol.FileInfo {
	f := ldbGet(s.db, []byte(s.folder), device[:], []byte(normalizedFilename(file)))
	f.Name = nativeFilename(f.Name)
	return f
}

func (s *Set) GetGlobal(file string) protocol.FileInfo {
	f := ldbGetGlobal(s.db, []byte(s.folder), []byte(normalizedFilename(file)))
	f.Name = nativeFilename(f.Name)
	return f
}

func (s *Set) Availability(file string) []protocol.DeviceID {
	return ldbAvailability(s.db, []byte(s.folder), []byte(normalizedFilename(file)))
}

func (s *Set) LocalVersion(device protocol.DeviceID) uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.localVersion[device]
}

// ListFolders returns the folder IDs seen in the database.
func ListFolders(db *leveldb.DB) []string {
	return ldbListFolders(db)
}

// DropFolder clears out all information related to the given folder from the
// database.
func DropFolder(db *leveldb.DB, folder string) {
	ldbDropFolder(db, []byte(folder))
}

func normalizeFilenames(fs []protocol.FileInfo) {
	for i := range fs {
		fs[i].Name = normalizedFilename(fs[i].Name)
	}
}

func nativeFileIterator(fn fileIterator) fileIterator {
	return func(fi protocol.FileIntf) bool {
		switch f := fi.(type) {
		case protocol.FileInfo:
			f.Name = nativeFilename(f.Name)
			return fn(f)
		case protocol.FileInfoTruncated:
			f.Name = nativeFilename(f.Name)
			return fn(f)
		default:
			panic("unknown interface type")
		}
	}
}
