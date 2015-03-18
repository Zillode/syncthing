// Copyright (C) 2014 The Syncthing Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License along
// with this program. If not, see <http://www.gnu.org/licenses/>.

package cache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/syncthing/protocol"
)

type FolderSize struct {
	folder      string
	globalSize  map[protocol.DeviceID]*int64
	globalCount map[protocol.DeviceID]*int64
	haveSize    map[protocol.DeviceID]*int64
	haveCount   map[protocol.DeviceID]*int64
	needSize    map[protocol.DeviceID]*int64
	needCount   map[protocol.DeviceID]*int64
	mut         sync.RWMutex // protects the above
}

func NewFolderSize(folder string) *FolderSize {
	fs := FolderSize{
		folder:      folder,
		globalSize:  make(map[protocol.DeviceID]*int64),
		globalCount: make(map[protocol.DeviceID]*int64),
		haveSize:    make(map[protocol.DeviceID]*int64),
		haveCount:   make(map[protocol.DeviceID]*int64),
		needSize:    make(map[protocol.DeviceID]*int64), // TODO not a map?
		needCount:   make(map[protocol.DeviceID]*int64), // TODO not a map?
	}
	return &fs
}

func (fs *FolderSize) AddHave(device protocol.DeviceID, nfiles int64, bytes int64) {
	fs.mut.Lock()
	pFiles, ok := fs.haveCount[device]
	pBytes, _ := fs.haveSize[device]
	if !ok {
		fs.haveCount[device] = &nfiles
		fs.haveSize[device] = &bytes
		fs.mut.Unlock()
		fmt.Println("Init cached have size", fs.folder, device.String()[1:7], nfiles, bytes)
		return
	}
	fs.mut.Unlock()
	atomic.AddInt64(pFiles, nfiles)
	atomic.AddInt64(pBytes, bytes)
	fmt.Println("Add cached have size", fs.folder, device.String()[1:7], nfiles, bytes, " total: ", atomic.LoadInt64(pFiles), atomic.LoadInt64(pBytes))
}

func (fs *FolderSize) AddGlobal(device protocol.DeviceID, nfiles int64, bytes int64) {
	fs.mut.Lock()
	pFiles, ok := fs.globalCount[device]
	pBytes, _ := fs.globalSize[device]
	if !ok {
		fs.globalCount[device] = &nfiles
		fs.globalSize[device] = &bytes
		fs.mut.Unlock()
		fmt.Println("Init cached global size", fs.folder, device.String()[1:7], nfiles, bytes)
		return
	}
	fs.mut.Unlock()
	atomic.AddInt64(pFiles, nfiles)
	atomic.AddInt64(pBytes, bytes)
	fmt.Println("Add cached global size", fs.folder, device.String()[1:7], nfiles, bytes, " total: ", atomic.LoadInt64(pFiles), atomic.LoadInt64(pBytes))
}

func (fs *FolderSize) GetHave(device protocol.DeviceID) (int64, int64) {
	fs.mut.RLock()
	pFiles, ok := fs.haveCount[device]
	pBytes, _ := fs.haveSize[device]
	fs.mut.RUnlock()
	if !ok {
		panic("Cached size not initialised yet " + fs.folder)
	}
	return atomic.LoadInt64(pFiles), atomic.LoadInt64(pBytes)
}

func (fs *FolderSize) GetGlobal() (int64, int64) {
	nfiles := int64(0)
	bytes := int64(0)
	fs.mut.RLock()
	for _, pF := range fs.globalCount {
		f := atomic.LoadInt64(pF)
		if f > nfiles {
			nfiles = f
		}
	}
	for _, pB := range fs.globalSize {
		b := atomic.LoadInt64(pB)
		if b > bytes {
			bytes = b
		}
	}
	fs.mut.RUnlock()
	return nfiles, bytes
}
