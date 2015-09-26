// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package scanner

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

var blocksTestData = []struct {
	data      []byte
	blocksize int
	hash      []string
}{
	{[]byte(""), 1024, []string{
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
	{[]byte("contents"), 1024, []string{
		"48a4e3a77de0efa3c4e9469c029a64ef"}},
	{[]byte("contents"), 9, []string{
		"48a4e3a77de0efa3c4e9469c029a64ef"}},
	{[]byte("contents"), 8, []string{
		"48a4e3a77de0efa3c4e9469c029a64ef"}},
	{[]byte("contents"), 7, []string{
		"5a8763734349fda08643a38b84ae409d",
		"7c210a41b7111c43d08129b05e349af2"},
	},
	{[]byte("contents"), 3, []string{
		"af236450a3fca4241682bde68a98b1f5",
		"a794a70d63694613bd0cbb6cc50c3354",
		"5eedd70d9e0f2673d744e57741abac8e"},
	},
	{[]byte("conconts"), 3, []string{
		"af236450a3fca4241682bde68a98b1f5",
		"af236450a3fca4241682bde68a98b1f5",
		"5eedd70d9e0f2673d744e57741abac8e"},
	},
	{[]byte("contenten"), 3, []string{
		"af236450a3fca4241682bde68a98b1f5",
		"a794a70d63694613bd0cbb6cc50c3354",
		"a794a70d63694613bd0cbb6cc50c3354"},
	},
}

func TestBlocks(t *testing.T) {
	for _, test := range blocksTestData {
		buf := bytes.NewBuffer(test.data)
		blocks, err := Blocks(buf, test.blocksize, 0, nil)

		if err != nil {
			t.Fatal(err)
		}

		if l := len(blocks); l != len(test.hash) {
			t.Fatalf("Incorrect number of blocks %d != %d", l, len(test.hash))
		} else {
			i := 0
			for off := int64(0); off < int64(len(test.data)); off += int64(test.blocksize) {
				if blocks[i].Offset != off {
					t.Errorf("Incorrect offset for block %d: %d != %d", i, blocks[i].Offset, off)
				}

				bs := test.blocksize
				if rem := len(test.data) - int(off); bs > rem {
					bs = rem
				}
				if int(blocks[i].Size) != bs {
					t.Errorf("Incorrect length for block %d: %d != %d", i, blocks[i].Size, bs)
				}
				if h := fmt.Sprintf("%x", blocks[i].Hash); h != test.hash[i] {
					t.Errorf("Incorrect block hash %q != %q", h, test.hash[i])
				}

				i++
			}
		}
	}
}

var diffTestData = []struct {
	a string
	b string
	s int
	d []protocol.BlockInfo
}{
	{"contents", "contents", 1024, []protocol.BlockInfo{}},
	{"", "", 1024, []protocol.BlockInfo{}},
	{"contents", "contents", 3, []protocol.BlockInfo{}},
	{"contents", "cantents", 3, []protocol.BlockInfo{{0, 3, nil}}},
	{"contents", "contants", 3, []protocol.BlockInfo{{3, 3, nil}}},
	{"contents", "cantants", 3, []protocol.BlockInfo{{0, 3, nil}, {3, 3, nil}}},
	{"contents", "", 3, []protocol.BlockInfo{{0, 0, nil}}},
	{"", "contents", 3, []protocol.BlockInfo{{0, 3, nil}, {3, 3, nil}, {6, 2, nil}}},
	{"con", "contents", 3, []protocol.BlockInfo{{3, 3, nil}, {6, 2, nil}}},
	{"contents", "con", 3, nil},
	{"contents", "cont", 3, []protocol.BlockInfo{{3, 1, nil}}},
	{"cont", "contents", 3, []protocol.BlockInfo{{3, 3, nil}, {6, 2, nil}}},
}

func TestDiff(t *testing.T) {
	for i, test := range diffTestData {
		a, _ := Blocks(bytes.NewBufferString(test.a), test.s, 0, nil)
		b, _ := Blocks(bytes.NewBufferString(test.b), test.s, 0, nil)
		_, d := BlockDiff(a, b)
		if len(d) != len(test.d) {
			t.Fatalf("Incorrect length for diff %d; %d != %d", i, len(d), len(test.d))
		} else {
			for j := range test.d {
				if d[j].Offset != test.d[j].Offset {
					t.Errorf("Incorrect offset for diff %d block %d; %d != %d", i, j, d[j].Offset, test.d[j].Offset)
				}
				if d[j].Size != test.d[j].Size {
					t.Errorf("Incorrect length for diff %d block %d; %d != %d", i, j, d[j].Size, test.d[j].Size)
				}
			}
		}
	}
}
