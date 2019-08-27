/*
Copyright (c) 2019 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package pgbadge

import (
	"github.com/jackc/pgx"
	"context"
	"io"
)

/*
Generically allows access to *Tx, *Conn and *ConnPool

	var _ IQueryable = (*pgx.ConnPool)(nil)
	var _ IQueryable = (*pgx.Conn)(nil)
	var _ IQueryable = (*pgx.Tx)(nil)
*/
type IQueryable interface {
	BeginBatch() *pgx.Batch
	CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error)
	CopyFromReader(r io.Reader, sql string) (commandTag pgx.CommandTag, err error)
	CopyToWriter(w io.Writer, sql string, args ...interface{}) (commandTag pgx.CommandTag, err error)
	
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	ExecEx(ctx context.Context, sql string, options *pgx.QueryExOptions, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	
	Prepare(name, sql string) (*pgx.PreparedStatement, error)
	PrepareEx(ctx context.Context, name, sql string, opts *pgx.PrepareExOptions) (*pgx.PreparedStatement, error)
	
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
	QueryEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (*pgx.Rows, error)
	
	QueryRow(sql string, args ...interface{}) *pgx.Row
	QueryRowEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) *pgx.Row
}

var _ IQueryable = (*pgx.ConnPool)(nil)
var _ IQueryable = (*pgx.Conn)(nil)
var _ IQueryable = (*pgx.Tx)(nil)

/*
Generically allows access to *Conn and *ConnPool

	var _ IConn = (*pgx.ConnPool)(nil)
	var _ IConn = (*pgx.Conn)(nil)
*/
type IConn interface {
	IQueryable
	Begin() (*pgx.Tx, error)
	BeginEx(ctx context.Context, txOptions *pgx.TxOptions) (*pgx.Tx, error)
}

var _ IConn = (*pgx.ConnPool)(nil)
var _ IConn = (*pgx.Conn)(nil)


