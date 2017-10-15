/*
MIT License

Copyright (c) 2017 Simon Schmidt

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


package oldbolt

import "github.com/boltdb/bolt"
import "github.com/byte-mug/fastnntp/posting"

import "github.com/maxymania/fastnntp-polyglot"

type Caps struct{
	Stamper posting.Stamper
	GroupHeadDB newspolyglot.GroupHeadDB
	GroupHeadCache newspolyglot.GroupHeadCache
	ArticlePostingDB newspolyglot.ArticlePostingDB
	ArticleDirectDB newspolyglot.ArticleDirectDB
	ArticleGroupDB newspolyglot.ArticleGroupDB
	GroupRealtimeDB newspolyglot.GroupRealtimeDB
	GroupStaticDB newspolyglot.GroupStaticDB
}

type Articledb struct{
	DB *bolt.DB
	Stamper posting.Stamper
	Caps Caps
}

// Utility to implement third party Interfaces
type Wrapper struct{
	*Articledb
}

type articleTransaction struct{
	db *Articledb
	tx *bolt.Tx
	writable bool
	done bool
}
func (a *Articledb) begin(writable bool) (*articleTransaction,error) {
	tx,e := a.DB.Begin(writable)
	if e!=nil { return nil,e }
	return &articleTransaction{a,tx,writable,false},nil
}
func (a *articleTransaction) commit() error {
	if a.done { return nil }
	a.done = true
	if !a.writable { return a.tx.Rollback() }
	return a.tx.Commit()
}
func (a *articleTransaction) rollback() error {
	if a.done { return nil }
	a.done = true
	return a.tx.Rollback()
}

func (a *Articledb) Initialize() {
	a.DB.Update(initializeDB)
	
	w := &Wrapper{a}
	if a.Caps.Stamper==nil { a.Caps.Stamper = a.Stamper }
	if a.Caps.GroupHeadDB==nil { a.Caps.GroupHeadDB = w }
	if a.Caps.GroupHeadCache==nil { a.Caps.GroupHeadCache = w }
	if a.Caps.ArticlePostingDB==nil { a.Caps.ArticlePostingDB = w }
	if a.Caps.ArticleDirectDB==nil { a.Caps.ArticleDirectDB = w }
	if a.Caps.ArticleGroupDB==nil { a.Caps.ArticleGroupDB = w }
	if a.Caps.GroupRealtimeDB==nil { a.Caps.GroupRealtimeDB = w }
	if a.Caps.GroupStaticDB==nil { a.Caps.GroupStaticDB = w }
	
}
var tablesToCreate = [][]byte{
	tGRPNUMS,
	tGRPINFO,
	tGRPARTS,
	tARTMETA,
	tARTHEAD,
	tARTBODY,
	tARTOVER,
}
func initializeDB(tx *bolt.Tx) error {
	for _,name := range tablesToCreate {
		tx.CreateBucketIfNotExists(name)
	}
	return nil
}

// "Tables" AKA Buckets

// Groups
var tGRPNUMS = []byte("grpnums")
var tGRPINFO = []byte("grpinfo")
var tGRPARTS = []byte("grparts")

// Articles
var tARTMETA = []byte("artmeta")
var tARTHEAD = []byte("arthead")
var tARTBODY = []byte("artbody")
var tARTOVER = []byte("artover")


// Funcs
func encode64(num int64) []byte{
	b := make([]byte,8)
	for i := 7; i>=0 ; i-- {
		b[i] = byte(num&0xff)
		num>>=8
	}
	return b
}
func decode64(buf []byte) (r int64) {
	for _,b := range buf {
		r = (r<<8)|int64(b)
	}
	return
}

