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

// Authenticated Posting
package postauth

type AuthRank uint8
const (
	ARReader AuthRank = iota
	ARUser
	ARModerator
	ARFeeder
)
func (rank AuthRank) TestStatus(status byte) (ok bool){
	switch status {
	case 'y':
		ok = ARUser     <=rank
	case 'm':
		ok = ARModerator<=rank
	default: /* 'n' */
		ok = ARFeeder   <=rank
	}
	return
}

// Cacheable Group-Head
type GroupHeadCacheWithAuth interface{
	// Filters the list "groups" removing those groups that
	// eighter not exist or they can't be posted to.
	//
	// rank specifies the poster's authentication rank.
	GroupHeadFilterWithAuth(rank AuthRank,groups [][]byte) ([][]byte,error)
}

// Implements newspolyglot.GroupHeadCache
type GroupHeadCacheAuthed struct{
	Base GroupHeadCacheWithAuth
	Rank AuthRank
}

// Filters the list "groups" removing those groups that
// eighter not exist or they can't be posted to.
func (a *GroupHeadCacheAuthed) GroupHeadFilter(groups [][]byte) ([][]byte,error) {
	return a.Base.GroupHeadFilterWithAuth(a.Rank,groups)
}


