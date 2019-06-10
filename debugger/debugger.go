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


package debugger

import "fmt"
import "log"
import "github.com/maxymania/fastnntp-polyglot"

func StringifyArticle(aobj *newspolyglot.ArticleObject) string {
	if aobj==nil { return "<nil>" }
	return fmt.Sprintf("{%p %p}",aobj.Head,aobj.Body)
}

func StringifyOverview(aov *newspolyglot.ArticleOverview) string {
	if aov==nil { return "<nil>" }
	return fmt.Sprintf("{%q %q %q %q %q %v %v}",aov.Subject, aov.From, aov.Date, aov.MsgId, aov.Refs, aov.Bytes, aov.Lines)
}


type AgDB struct {
	newspolyglot.ArticleGroupDB
}
func (a AgDB) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	b,o := a.ArticleGroupDB.ArticleGroupGet(group,num,head,body,id_buf)
	log.Printf("ArticleGroupGet(%q %d %v %v) -> %q %s",group,num,head,body,b,StringifyArticle(o))
	return b,o
}
func (a AgDB) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	b,o := a.ArticleGroupDB.ArticleGroupStat(group,num,id_buf)
	log.Printf("ArticleGroupStat(%q %d) -> %q %v",group,num,b,o)
	return b,o
}
func (a AgDB) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	ni,id,ok = a.ArticleGroupDB.ArticleGroupMove(group,i,backward,id_buf)
	log.Printf("ArticleGroupMove(%q %d %v) -> %d %q %v",group,i,backward,ni,id,ok)
	return
}

type AdDB struct {
	newspolyglot.ArticleDirectDB
}


func (a AdDB) ArticleDirectStat(id []byte) bool {
	ok := a.ArticleDirectDB.ArticleDirectStat(id)
	log.Printf("ArticleDirectStat(%q) -> %v",id,ok)
	return ok
}
func (a AdDB) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	o := a.ArticleDirectDB.ArticleDirectGet(id,head, body)
	log.Printf("ArticleDirectGet(%q %v %v) -> %v",id,head, body, StringifyArticle(o))
	return o
}
func (a AdDB) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	o := a.ArticleDirectDB.ArticleDirectOverview(id)
	log.Printf("ArticleDirectOverview(%q) -> %v",id, StringifyOverview(o))
	return o
}

