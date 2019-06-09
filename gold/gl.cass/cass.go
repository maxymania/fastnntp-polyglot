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


package glcass

import "bytes"
import "github.com/gocql/gocql"
import "github.com/maxymania/fastnntp-polyglot/postauth"
import rb "github.com/emirpasic/gods/trees/redblacktree"

import "github.com/maxymania/fastnntp-polyglot/gold"

func Initialize(session *gocql.Session) {
	session.Query(`
	CREATE TABLE IF NOT EXISTS grouplist (
		groupname blob PRIMARY KEY,
		status tinyint,
		descr blob
	)
	`).Exec()
}

func bcmp(a,b interface{}) int { return bytes.Compare(a.([]byte),b.([]byte)) }

type iter struct {
	*gocql.Iter
	Q *gocql.Query
}

func (i *iter) place(o iter) {
	i.sClose()
	*i = o
}
func (i *iter) sClose() error {
	if i.Iter==nil { return nil }
	return i.Close()
}
func (i iter) Close() error {
	i.Q.Release()
	return i.Iter.Close()
}
func (i iter) scanclose(dest ...interface{}) bool {
	defer i.Close()
	ok := i.Scan(dest...)
	return ok
}

func qIter(q *gocql.Query) iter {
	return iter{q.Iter(),q}
}

func qExec(q *gocql.Query) error {
	defer q.Release()
	return q.Exec()
}

type Database struct {
	Session  *gocql.Session
	OnUpsert gocql.Consistency
}

func (d *Database) AddGroupDescr(group, descr []byte) error {
	return qExec(d.Session.Query(`UPDATE grouplist SET descr = ? WHERE groupname = ?`,descr,group).Consistency(d.OnUpsert))
}
func (d *Database) AddGroupStatus(group []byte, status byte) error {
	return qExec(d.Session.Query(`UPDATE grouplist SET status = ? WHERE groupname = ?`,status,group).Consistency(d.OnUpsert))
}

func (d *Database) GroupHeadFilterWithAuth(rank postauth.AuthRank, groups [][]byte) ([][]byte, error) {
	/* ARReader cannot write at all. Nothing to do. */
	if rank==postauth.ARReader { return nil,nil }
	
	iter := qIter(d.Session.Query(`SELECT groupname,status FROM grouplist WHERE groupname IN ?`,groups))
	
	groups = groups[:0]
	var GRP []byte
	var GS  byte
	for iter.Scan(&GRP,&GS) {
		if GS!=0 && rank.TestStatus(GS) {
			groups = append(groups,GRP)
		}
		GRP = nil
		GS = 0
	}
	
	return groups,iter.Close()
}

func (d *Database) GroupBaseList(status, descr bool,targ func(group []byte, status byte, descr []byte)) bool {
	var GRP []byte
	var GS byte
	var GD []byte
	tree := rb.NewWith(bcmp)
	
	if !descr {
		iter := qIter(d.Session.Query(`SELECT groupname,status FROM grouplist`))
		defer iter.Close()
		for iter.Scan(&GRP,&GS) {
			if GS==0 { continue }
			tree.Put(GRP,GS)
			GRP = nil
			GS = 0
		}
		ti := tree.Iterator()
		for b := ti.First() ; b ; b = ti.Next() {
			targ(ti.Key().([]byte),ti.Value().(byte),nil)
		}
		return true
	} else if !status {
		iter := qIter(d.Session.Query(`SELECT groupname,descr FROM grouplist`))
		defer iter.Close()
		for iter.Scan(&GRP,&GD) {
			tree.Put(GRP,GD)
			GRP = nil
			GD = nil
		}
		ti := tree.Iterator()
		for b := ti.First() ; b ; b = ti.Next() {
			targ(ti.Key().([]byte),0,ti.Value().([]byte))
		}
		return true
	} else {
		return false
	}
}

var _ gold.GroupListDB = (*Database)(nil)

/* ## */
