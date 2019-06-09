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


package gold

import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot/postauth"


type ArticleGroupEX interface {
	newspolyglot.ArticleGroupDB
	StoreArticleInfos(groups [][]byte, nums []int64, exp uint64, ov *newspolyglot.ArticleOverview) (err error)
	GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool)
}

type ArticleGroupWrapper struct {
	ArticleGroupEX
	Direct newspolyglot.ArticleDirectDB
}
func (a *ArticleGroupWrapper) ArticleGroupGet(group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	id,ok := a.ArticleGroupStat(group,num,id_buf)
	if !ok { return nil,nil }
	ao := a.Direct.ArticleDirectGet(id,head,body)
	if ao==nil { return nil,nil }
	return id,ao
}

type GroupListDB interface {
	AddGroupDescr(group, descr []byte) error
	AddGroupStatus(group []byte, status byte) error
	
	GroupHeadFilterWithAuth(rank postauth.AuthRank, groups [][]byte) ([][]byte, error)
	GroupBaseList(status, descr bool,targ func(group []byte, status byte, descr []byte)) bool
}

