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
Cassandra backend for authentication.
*/
package cassauth

import (
	"github.com/gocql/gocql"
	
	"github.com/maxymania/fastnntp-polyglot/auth/pwdhash"
	
	"github.com/maxymania/fastnntp-polyglot/postauth"
	"github.com/maxymania/fastnntp-polyglot/postauth/advauth"
)


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
	CREATE TABLE IF NOT EXISTS authtable (
		username blob PRIMARY KEY,
		password blob,
		authrank tinyint
	)
	`).Exec()
}

type CassLoginHook struct {
	DB *gocql.Session
	Hash pwdhash.PasswordHash
	HashCost int
}

func (lh *CassLoginHook) InitHash(name string) (err error) {
	lh.Hash,err = pwdhash.GetPasswordHash(name)
	return
}
func (lh *CassLoginHook) hashObject() pwdhash.PasswordHash {
	ph := lh.Hash
	if ph!=nil { return ph }
	ph = pwdhash.DefaultPasswordHash()
	lh.Hash = ph
	return ph
}
func (lh *CassLoginHook) compareHashAndPassword(hashedPassword, password []byte) bool {
	return lh.hashObject().CompareHashAndPassword(hashedPassword,password)==nil
}
func (lh *CassLoginHook) generateFromPassword(password []byte) ([]byte, error) {
	return lh.hashObject().GenerateFromPassword(password,lh.HashCost)
}

func (lh *CassLoginHook) InsertUser(user, password []byte,rank postauth.AuthRank) error {
	hash,err := lh.generateFromPassword(password)
	if err!=nil { return err }
	return qExec(lh.DB.Query(`INSERT INTO authtable (username,password,authrank) VALUES (?,?,?) IF NOT EXISTS`,user,hash,uint8(rank)))
}
func (lh *CassLoginHook) UpdateUserPassword(user, password []byte) error {
	hash,err := lh.generateFromPassword(password)
	if err!=nil { return err }
	return qExec(lh.DB.Query(`UPDATE authtable SET password = ? WHERE username = ? IF EXISTS`,hash,user))
}
func (lh *CassLoginHook) UpdateUserRank(user []byte, rank postauth.AuthRank) error {
	return qExec(lh.DB.Query(`UPDATE authtable SET authrank = ? WHERE username = ? IF EXISTS`,uint8(rank),user))
}



/*
func (lh *CassLoginHook) CheckUser(user []byte) bool {
	lh.DB.Query(`SELECT password FROM authtable WHILE username = ?`,user)
	var x []byte
	return qIter(lh.DB.Query(`SELECT password FROM authtable WHERE username = ?`,user)).scanclose(&x)
}
*/


// Not implemented.
func (lh *CassLoginHook) AuthUserOnly(user []byte) (postauth.AuthRank,bool) { return 0,false }
func (lh *CassLoginHook) AuthUserPass(user, password []byte) (postauth.AuthRank,bool) {
	var pwdhash []byte
	var rank uint8
	if !qIter(lh.DB.Query(`SELECT password,authrank FROM authtable WHERE username = ?`,user)).scanclose(&pwdhash,&rank) { return 0,false }
	if !lh.compareHashAndPassword(pwdhash,password) { return 0,false }
	return postauth.AuthRank(rank),true
}

var _ advauth.LoginHook = (*CassLoginHook)(nil)


