// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package db

import (
	"database/sql"
	"fmt"
	"path/filepath"

	//_ "github.com/mxk/go-sqlite/sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/syncthing/protocol"
)

var connectionSetup = []string{
	`PRAGMA synchronous = NORMAL`,
	`PRAGMA foreign_keys = ON`,
}

var schemaSetup = []string{
	`CREATE TABLE IF NOT EXISTS File (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Device BLOB NOT NULL,
		Folder TEXT NOT NULL,
		Name TEXT NOT NULL,
		Flags INTEGER NOT NULL,
		Modified INTEGER NOT NULL,
		Version BLOB NOT NULL,
		Updated BOOLEAN NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS DeviceFolderNameIdx ON File (Device, Folder, Name)`,
	`CREATE INDEX IF NOT EXISTS NameVersionIdx ON File (Name, Version)`,
	`CREATE TABLE IF NOT EXISTS Block (
		Hash BLOB NOT NULL,
		FileID INTEGER NOT NULL REFERENCES File(ID) ON DELETE CASCADE,
		Size INTEGER NOT NULL,
		Offs INTEGER NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS HashIdx ON Block (Hash)`,
	`CREATE INDEX IF NOT EXISTS FileIDIdx ON Block (FileID)`,
}

var preparedStmts = [][2]string{
	{"selectFileID",
		"SELECT ID, Version FROM File WHERE Device==? AND Folder==? AND Name==?"},
	{"selectFileAll",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE Device==? AND Folder==? AND Name==?"},
	{"selectFileAllID",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE ID==?"},
	{"selectFileAllVersion",
		fmt.Sprintf("SELECT ID, Name, Flags, Modified, Version FROM File WHERE Name==? AND Version==? AND Flags & %d == 0", protocol.FlagInvalid)},
	{"deleteFile",
		"DELETE FROM File WHERE ID==?"},
	{"updateFile",
		"UPDATE File SET Updated=1 WHERE ID==?"},
	{"deleteBlocksFor",
		"DELETE FROM Block WHERE FileID==?"},
	{"insertFile",
		"INSERT INTO File (Device, Folder, Name, Flags, Modified, Version, Updated) VALUES (?, ?, ?, ?, ?, ?, 1)"},
	{"insertBlock",
		"INSERT INTO Block VALUES (?, ?, ?, ?)"},
	{"selectBlock",
		"SELECT Hash, Size, Offs FROM Block WHERE FileID==?"},
	{"selectFileHave",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE Device==? AND Folder==?"},
	{"selectFileGlobal",
		fmt.Sprintf("SELECT ID, Name, Flags, Modified, MAX(Version) FROM File WHERE Folder==? AND Flags & %d == 0 GROUP BY Name ORDER BY Name", protocol.FlagInvalid)},
	{"selectFilePrefixedGlobal",
		fmt.Sprintf("SELECT ID, Name, Flags, Modified, MAX(Version) FROM File WHERE Folder==? AND Flags & %d == 0 Name LIKE '?%' GROUP BY Name ORDER BY Name", protocol.FlagInvalid)},
	{"selectMaxID",
		"SELECT MAX(ID) FROM File WHERE Device==? AND Folder==?"},
	{"selectGlobalID",
		fmt.Sprintf("SELECT MAX(ID) FROM File WHERE Folder==? AND Name==? AND Flags & %d == 0", protocol.FlagInvalid)},
	{"selectMaxVersion",
		"SELECT MAX(Version) FROM File WHERE Folder==? AND Name==?"},
	{"selectWithVersion",
		"SELECT Device, Flags FROM File WHERE Folder==? AND Name==? AND Version==?"},
	{"selectNeed",
		`SELECT Name, MAX(Version) Version FROM File WHERE Folder==? GROUP BY Name EXCEPT
			SELECT Name, Version FROM File WHERE Device==? AND Folder==?`},
}

//type fileVersion struct {
//	version protocol.Vector
//	device  []byte
//}
//
//type versionList struct {
//	versions []fileVersion
//}
//
//func (l versionList) String() string {
//	var b bytes.Buffer
//	var id protocol.DeviceID
//	b.WriteString("{")
//	for i, v := range l.versions {
//		if i > 0 {
//			b.WriteString(", ")
//		}
//		copy(id[:], v.device)
//		fmt.Fprintf(&b, "{%d, %v}", v.version, id)
//	}
//	b.WriteString("}")
//	return b.String()
//}
//
//type fileList []protocol.FileInfo
//
//func (l fileList) Len() int {
//	return len(l)
//}
//
//func (l fileList) Swap(a, b int) {
//	l[a], l[b] = l[b], l[a]
//}
//
//func (l fileList) Less(a, b int) bool {
//	return l[a].Name < l[b].Name
//}

type MainDB struct {
	dir string
}

func NewMainDB(dir string) *MainDB {
	return &MainDB{
		dir: dir,
	}
}

func (m *MainDB) NewSQLiteDB(name string) (*SQLiteDB, error) {
	return NewSQLiteDB(filepath.Join(m.dir, name))
}

type SQLiteDB struct {
	db    *sql.DB
	stmts map[string]*sql.Stmt
}

func NewSQLiteDB(name string) (*SQLiteDB, error) {
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, err
	}

	for _, stmt := range connectionSetup {
		_, err = db.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	for _, stmt := range schemaSetup {
		_, err = tx.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	fdb := SQLiteDB{
		db:    db,
		stmts: make(map[string]*sql.Stmt),
	}

	for _, prep := range preparedStmts {
		stmt, err := db.Prepare(prep[1])
		if err != nil {
			return nil, err
		}
		fdb.stmts[prep[0]] = stmt
	}

	return &fdb, nil
}

func (db *SQLiteDB) update(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		panic(err)
	}

	db.updateTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *SQLiteDB) updateTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	for _, f := range fs {
		var id int64
		var version protocol.Vector

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		//if f.IsInvalid() {
		//	// Force an update
		//	version = 0
		//}

		if err == nil && !version.Equal(f.Version) {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
			_, err = tx.Stmt(db.stmts["deleteBlocksFor"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err == nil && version.Equal(f.Version) {
			_, err = tx.Stmt(db.stmts["updateFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if version.Equal(f.Version) {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				panic(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	return nil
}

func (db *SQLiteDB) replace(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		panic(err)
	}

	db.replaceTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *SQLiteDB) replaceTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE File SET Updated==0 WHERE Device==? AND Folder==?", device[:], folder)
	if err != nil {
		panic(err)
	}

	for _, f := range fs {
		var id int64
		var version protocol.Vector

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && !version.Equal(f.Version) {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
			_, err = tx.Stmt(db.stmts["deleteBlocksFor"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if !version.Equal(f.Version) {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				panic(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	_, err = tx.Exec("DELETE FROM File WHERE Folder==? AND Device==? AND Updated==0", folder, device[:])
	if err != nil {
		panic(err)
	}

	return nil
}

func (db *SQLiteDB) have(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectFileHave"].Query(device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) haveTruncated(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectFileHave"].Query(device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f FileInfoTruncated
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) global(folder string, fn Iterator) {
	rows, err := db.stmts["selectFileGlobal"].Query(folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) globalTruncated(folder string, fn Iterator) {
	rows, err := db.stmts["selectFileGlobal"].Query(folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f FileInfoTruncated
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) prefixedGlobalTruncated(folder, prefix string, fn Iterator) {
	rows, err := db.stmts["selectFileGlobal"].Query(folder, prefix)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f FileInfoTruncated
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) need(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectNeed"].Query(folder, device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var version protocol.Vector
		var id int64
		err = rows.Scan(&name, &version)
		if err != nil {
			panic(err)
		}

		var f protocol.FileInfo
		row := db.stmts["selectFileAllVersion"].QueryRow(name, version)
		err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err == sql.ErrNoRows {
			// There was no file to need, maybe because they're all marked invalid
			continue
		}
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) needTruncated(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectNeed"].Query(folder, device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var version protocol.Vector
		var id int64
		err = rows.Scan(&name, &version)
		if err != nil {
			panic(err)
		}

		var f FileInfoTruncated
		row := db.stmts["selectFileAllVersion"].QueryRow(name, version)
		err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		f.LocalVersion = int64(id)
		if err == sql.ErrNoRows {
			// There was no file to need, maybe because they're all marked invalid
			continue
		}
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *SQLiteDB) get(folder string, device protocol.DeviceID, name string) (protocol.FileInfo, bool) {
	var f protocol.FileInfo
	var id sql.NullInt64

	row := db.stmts["selectFileAll"].QueryRow(device[:], folder, name)
	err := row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	f.LocalVersion = int64(id.Int64)
	if !id.Valid {
		return protocol.FileInfo{}, false
	}
	if err != nil {
		panic(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id.Int64)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f, true
}

func (db *SQLiteDB) getGlobal(folder, name string) (protocol.FileInfo, bool) {
	var id sql.NullInt64

	row := db.stmts["selectGlobalID"].QueryRow(folder, name)
	err := row.Scan(&id)
	if !id.Valid {
		return protocol.FileInfo{}, false
	}
	if err != nil {
		panic(err)
	}

	var f protocol.FileInfo
	row = db.stmts["selectFileAllID"].QueryRow(id.Int64)
	err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	f.LocalVersion = int64(id.Int64)
	if !id.Valid {
		return protocol.FileInfo{}, false
	}
	if err != nil {
		panic(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id.Int64)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f, true
}

func (db *SQLiteDB) getGlobalTruncated(folder, name string) (FileInfoTruncated, bool) {
	var id sql.NullInt64

	row := db.stmts["selectGlobalID"].QueryRow(folder, name)
	err := row.Scan(&id)
	if !id.Valid {
		return FileInfoTruncated{}, false
	}
	if err != nil {
		panic(err)
	}

	var f FileInfoTruncated
	row = db.stmts["selectFileAllID"].QueryRow(id)
	err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	f.LocalVersion = int64(id.Int64)
	if !id.Valid {
		return f, false
	}
	if err != nil {
		panic(err)
	}

	return f, true
}

func (db *SQLiteDB) maxID(folder string, device protocol.DeviceID) int64 {
	var id sql.NullInt64

	row := db.stmts["selectMaxID"].QueryRow(device[:], folder)
	err := row.Scan(&id)
	if !id.Valid {
		l.Debugln(folder, device, "none")
		return 0
	}
	if err != nil {
		panic(err)
	}
	l.Debugln(folder, device, id)
	return int64(id.Int64)
}

func (db *SQLiteDB) availability(folder, name string) []protocol.DeviceID {
	var version protocol.Vector
	row := db.stmts["selectMaxVersion"].QueryRow(folder, name)
	err := row.Scan(&version)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		panic(err)
	}

	rows, err := db.stmts["selectWithVersion"].Query(folder, name, version)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		panic(err)
	}

	var available []protocol.DeviceID
	var device []byte
	var flags uint32
	for rows.Next() {
		err = rows.Scan(&device, &flags)
		if err != nil {
			panic(err)
		}
		if flags&(protocol.FlagDeleted|protocol.FlagInvalid) == 0 {
			available = append(available, protocol.DeviceIDFromBytes(device))
		}
	}

	return available
}
