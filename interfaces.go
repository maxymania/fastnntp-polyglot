/*
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

// Polyglot Usenet-News Storage Framework
package newspolyglot

import "github.com/byte-mug/fastnntp/posting"

// Cacheable Group-Head
type GroupHeadCache interface{
	// Filters the list "groups" removing those groups that
	// eighter not exist or they can't be posted to.
	GroupHeadFilter(groups [][]byte) ([][]byte,error)
}

type GroupHeadDB interface{
	GroupHeadInsert(groups [][]byte,buf []int64) ([]int64,error)
	GroupHeadRevert(groups [][]byte,nums []int64) error
}

type ArticlePostingDB interface{
	ArticlePostingPost(headp *posting.HeadInfo,body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool,err error)
	ArticlePostingCheckPost() (possible bool)
	ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool)
}

type ArticleObject struct{
	Head,Body []byte
	
	Bufs [2]*[]byte
}
type ArticleOverview struct{
	// This structure is fixed. It shall not change.
	Subject, From, Date, MsgId, Refs []byte
	Bytes, Lines int64
}

type ArticleDirectDB interface{
	ArticleDirectStat(id []byte) bool
	ArticleDirectGet(id []byte,head ,body bool) *ArticleObject
	ArticleDirectOverview(id []byte) *ArticleOverview
}
type ArticleGroupDB interface{
	ArticleGroupStat(group []byte, num int64,id_buf []byte) ([]byte, bool)
	ArticleGroupGet(group []byte, num int64,head ,body bool,id_buf []byte) ([]byte,*ArticleObject)
	ArticleGroupOverview(group []byte, first, last int64,targ func(*ArticleOverview))
	ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool)
	ArticleGroupList(group []byte, first, last int64,targ func(int64))
}

type GroupRealtimeDB interface{
	GroupRealtimeQuery(group []byte) (number int64,low int64,high int64,ok bool)
	GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool
}

type GroupStaticDB interface{
	GroupStaticList(targ func(group []byte, descr []byte)) bool
}

