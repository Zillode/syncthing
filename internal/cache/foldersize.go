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
	globalFiles *int64
	globalSize  *int64
	haveSize    map[protocol.DeviceID]*int64
	haveCount   map[protocol.DeviceID]*int64
	needSize    map[protocol.DeviceID]*int64
	needCount   map[protocol.DeviceID]*int64
	mut         sync.RWMutex // protects the above
}

func NewFolderSize(folder string) *FolderSize {
	globalFiles := int64(0)
	globalSize := int64(0)
	fs := FolderSize{
		folder:      folder,
		globalFiles: &globalFiles,
		globalSize:  &globalSize,
		haveSize:    make(map[protocol.DeviceID]*int64),
		haveCount:   make(map[protocol.DeviceID]*int64),
		needSize:    make(map[protocol.DeviceID]*int64), // TODO not a map?
		needCount:   make(map[protocol.DeviceID]*int64), // TODO not a map?
	}
	hSize := int64(0)
	hCount := int64(0)
	nSize := int64(0)
	nCount := int64(0)
	// Initialise local device
	fs.haveSize[protocol.LocalDeviceID] = &hSize
	fs.haveCount[protocol.LocalDeviceID] = &hCount
	fs.needSize[protocol.LocalDeviceID] = &nSize
	fs.needCount[protocol.LocalDeviceID] = &nCount
	return &fs
}

func (fs *FolderSize) AddHave(device protocol.DeviceID, nfiles int64, bytes int64) {
	fs.mut.Lock()
	pFiles, ok := fs.haveCount[device]
	pBytes, _ := fs.haveSize[device]
	fs.mut.Unlock()
	if !ok {
		nfiles := int64(0)
		bytes := int64(0)
		fs.haveCount[device] = &nfiles
		fs.haveSize[device] = &bytes
		return
	}
	atomic.AddInt64(pFiles, nfiles)
	atomic.AddInt64(pBytes, bytes)
	fmt.Println("Add cached have size", fs.folder, device, nfiles, bytes, " total: ", atomic.LoadInt64(pFiles), atomic.LoadInt64(pBytes))
}

func (fs *FolderSize) AddGlobal(nfiles int64, bytes int64) {
	atomic.AddInt64(fs.globalFiles, nfiles)
	atomic.AddInt64(fs.globalSize, bytes)
	fmt.Println("Add cached global", fs.folder, nfiles, bytes, " total: ", atomic.LoadInt64(fs.globalFiles), atomic.LoadInt64(fs.globalSize))
}

func (fs *FolderSize) GetHave(device protocol.DeviceID) (int64, int64) {
	fs.mut.Lock()
	pFiles, ok := fs.haveCount[device]
	pBytes, _ := fs.haveSize[device]
	fs.mut.Unlock()
	if !ok {
		panic("Cached size not initialised yet " + fs.folder)
	}
	return atomic.LoadInt64(pFiles), atomic.LoadInt64(pBytes)
}

func (fs *FolderSize) GetGlobal() (int64, int64) {
	return atomic.LoadInt64(fs.globalFiles), atomic.LoadInt64(fs.globalSize)
}
