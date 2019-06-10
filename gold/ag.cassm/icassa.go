/*
Copyright (c) 2018-2019 Simon Schmidt

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


package cassm

import "github.com/gocql/gocql"
import "errors"
import "time"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/byte-mug/golibs/msgpackx"

func flattenP(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{ &o.Subject,&o.From,&o.Date,&o.MsgId,&o.Refs,&o.Bytes,&o.Lines }
}
func flattenV(o *newspolyglot.ArticleOverview) []interface{} {
	return []interface{}{  o.Subject, o.From, o.Date, o.MsgId, o.Refs, o.Bytes, o.Lines }
}

func getMsgId(ovd,id_buf []byte) ([]byte,bool) {
	o := newspolyglot.AcquireArticleOverview()
	defer newspolyglot.ReleaseArticleOverview(o)
	o.MsgId = id_buf
	err := msgpackx.Unmarshal(ovd,flattenP(o)...)
	return o.MsgId,err==nil
}



var EUnimplemented = errors.New("EUnimplemented")
var ENoSuchGroup = errors.New("Missing Group")

func initGen(session *gocql.Session) {
	session.Query(`
	CREATE TABLE IF NOT EXISTS newsgroups (
		groupname blob PRIMARY KEY,
		identifier uuid
	)
	`).Exec()
	session.Query(`
	CREATE TABLE IF NOT EXISTS agrpcnt (
		identifier uuid,
		livesuntil bigint,
		number counter,
		PRIMARY KEY(identifier,livesuntil)
	)
	`).Exec()
}
func Initialize(session *gocql.Session) {
	initGen(session)
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat (
		identifier uuid,
		articlenum bigint,
		overview blob,
		PRIMARY KEY(identifier,articlenum)
	)
	`).Exec()
}
func InitializeN2Layer(session *gocql.Session) {
	initGen(session)
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat1l2 (
		identifier uuid,
		articlepart bigint,
		expiresat bigint,
		PRIMARY KEY(identifier,articlepart)
	)
	`).Exec()
	session.Query(`
	CREATE TABLE IF NOT EXISTS agstat2l2 (
		identifier uuid,
		articlepart bigint,
		articlenum bigint,
		overview blob,
		PRIMARY KEY((identifier,articlepart),articlenum)
	)
	`).Exec()
}

/*
Maintainance routine for the Countertable 'agrpcnt'.
*/
func MaintainanceCtrTable(session *gocql.Session) {
	iter := session.Query(`
		SELECT identifier,livesuntil FROM agrpcnt WHERE livesuntil < ? ALLOW FILTERING
	`,time.Now().UTC().Unix()).PageSize(24<<10).Iter()
	
	defer iter.Close()
	
	var gid gocql.UUID
	var lives uint64
	for iter.Scan(&gid,&lives) {
		session.Query(`DELETE FROM agrpcnt WHERE identifier = ? AND livesuntil = ?`,gid,lives).Exec()
	}
}

func getUUID(session *gocql.Session,name []byte) (u gocql.UUID,err error) {
	iter := session.Query(`
	SELECT identifier FROM newsgroups WHERE groupname = ?
	`,name).Consistency(gocql.One).Iter()
	ok := iter.Scan(&u)
	iter.Close()
	if !ok {
		u,err = gocql.RandomUUID()
		if err!=nil { return }
		session.Query(`
		INSERT INTO newsgroups (groupname,identifier) VALUES (?,?)
		IF NOT EXISTS
		`,name,u).Consistency(gocql.Any).Exec()
		iter = session.Query(`
		SELECT identifier FROM newsgroups WHERE groupname = ?
		`,name).Consistency(gocql.One).Iter()
		ok = iter.Scan(&u)
		iter.Close()
		if !ok {
			err = ENoSuchGroup
		}
	}
	return
}
func peekUUID(session *gocql.Session,name []byte) (u gocql.UUID,err error) {
	iter := session.Query(`
	SELECT identifier FROM newsgroups WHERE groupname = ?
	`,name).Consistency(gocql.One).Iter()
	ok := iter.Scan(&u)
	iter.Close()
	if !ok {
		err = ENoSuchGroup
	}
	return
}

type unimplemented struct{}

// Unimplemented!
func (unimplemented) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	return nil,nil
}

// Panic!
//func (unimplemented) ArticleGroupOverview(group []byte, first, last int64, targ func(int64, *newspolyglot.ArticleOverview)) { panic("...") }

