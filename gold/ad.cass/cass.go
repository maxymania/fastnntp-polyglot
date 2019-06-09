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


/*
Cassandra backend for gold.ArticleDirectEX.
*/
package adcass

import "github.com/gocql/gocql"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/gold"
import "github.com/byte-mug/golibs/msgpackx"
import "time"

func flattenP(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{ &o.Subject,&o.From,&o.Date,&o.MsgId,&o.Refs,&o.Bytes,&o.Lines }
}
func flattenV(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{  o.Subject, o.From, o.Date, o.MsgId, o.Refs, o.Bytes, o.Lines }
}

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

func Initialize(session *gocql.Session) {
	session.Query(`
	CREATE TABLE IF NOT EXISTS artdirtab (
		msgid blob PRIMARY KEY,
		xover blob,
		xhead blob,
		xbody blob
	)
	`).Exec()
}

type Storage struct {
	Session  *gocql.Session
	OnUpsert gocql.Consistency
}

func (s *Storage) ArticleDirectStat(id []byte) bool {
	var b []byte
	iter := qIter(s.Session.Query(`
	SELECT xover FROM artdirtab WHERE msgid = ?
	`,id))
	
	return iter.scanclose(&b)
}
func (s *Storage) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	hb := "xhead,xbody"
	if !head { hb = "xbody" } else if !body { hb = "xhead" }
	
	iter := qIter(s.Session.Query(`
	SELECT `+hb+` FROM artdirtab WHERE msgid = ?
	`,id))
	
	obj := newspolyglot.AcquireArticleObject()
	var ok bool
	if !head {
		ok = iter.scanclose(&obj.Body)
	} else if !body {
		ok = iter.scanclose(&obj.Head)
	} else {
		ok = iter.scanclose(&obj.Body,&obj.Head)
	}
	if !ok {
		newspolyglot.ReleaseArticleObject(obj)
	}
	
	return obj
}
func (s *Storage) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	var b []byte
	iter := qIter(s.Session.Query(`
	SELECT xover FROM artdirtab WHERE msgid = ?
	`,id))
	
	if !iter.scanclose(&b) { return nil }
	ov := newspolyglot.AcquireArticleOverview()
	
	if msgpackx.Unmarshal(b,flattenP(ov)...)!=nil { return nil }
	
	return ov
}

func (s *Storage) ArticleDirectStore(exp uint64, ov *newspolyglot.ArticleOverview,obj *newspolyglot.ArticleObject) (err error) {
	var over []byte
	over,err = msgpackx.Marshal(flattenV(ov)...)
	if err!=nil { return }
	
	secs := int64(time.Until(time.Unix(int64(exp),0))/time.Second) + 1
	
	q := s.Session.Query(`
	INSERT INTO artdirtab (msgid,xover,xhead,xbody) VALUES (?,?,?,?) USING TTL ?
	`,ov.MsgId,over,obj.Head,obj.Body,secs).Consistency(s.OnUpsert)
	defer q.Release()
	return q.Exec()
}

func (s *Storage) ArticleDirectRollback(id []byte) {
	q := s.Session.Query(`
	DELETE FROM artdirtab WHERE msgid = ?
	`,id).Consistency(s.OnUpsert)
	defer q.Release()
	q.Exec()
}

var _ gold.ArticleDirectEX = (*Storage)(nil)

