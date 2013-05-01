// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"io"
)

type mysqlField struct {
	name      string
	fieldType byte
	flags     fieldFlag
}

// mysqlRows is the driver-internal Rows struct that is never given to
// the database/sql package. This struct is 40 bytes on 64-bit
// machines and is recycled. Its size isn't very relevant, since we
// recycle it.
//
// Allocate with newMysqlRows (from buffer.go) and return with
// putMySQLRows.  See also: mysqlRowsI.
type mysqlRows struct {
	mc      *mysqlConn
	columns []mysqlField
	binary  bool // Note: packing small bool fields at the end
	eof     bool
}

// mysqlRowsI implements driver.Rows. Its wrapped *mysqlRows pointer
// becomes nil and recycled on Close. This struct is kept small (8
// bytes) to minimize garbage creation.
type mysqlRowsI struct {
	*mysqlRows
}

func (rows *mysqlRows) Columns() []string {
	if rows == nil {
		return nil
	}
	columns := make([]string, len(rows.columns))
	for i := range columns {
		columns[i] = rows.columns[i].name
	}
	return columns
}

func (ri *mysqlRowsI) Close() error {
	if ri.mysqlRows == nil {
		return nil // make Close() idempotent
	}

	err := ri.mysqlRows.close()
	putMysqlRows(ri.mysqlRows)
	ri.mysqlRows = nil
	return err
}

func (rows *mysqlRows) close() (err error) {
	// Remove unread packets from stream
	if !rows.eof {
		if rows.mc == nil || rows.mc.netConn == nil {
			return errInvalidConn
		}

		err = rows.mc.readUntilEOF()

		// explicitly set because readUntilEOF might return early in case of an
		// error
		rows.eof = true
	}

	rows.mc = nil

	if !rows.binary { // Binary rows use cached columns
		putFields(rows.columns)
	}
	rows.columns = nil

	return
}

func (rows *mysqlRows) Next(dest []driver.Value) (err error) {
	if rows == nil {
		return errInvalidConn
	}

	if rows.eof {
		return io.EOF
	}

	if rows.mc == nil || rows.mc.netConn == nil {
		return errInvalidConn
	}

	// Fetch next row from stream
	if rows.binary {
		err = rows.readBinaryRow(dest)
	} else {
		err = rows.readRow(dest)
	}

	if err == io.EOF {
		rows.eof = true
	}
	return err
}
