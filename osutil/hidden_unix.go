// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build !windows

package osutil

func HideFile(path string) error {
	return nil
}

func ShowFile(path string) error {
	return nil
}
