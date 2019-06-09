/*
Copyright (c) 2018 Simon Schmidt

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

// 24 Hours in seconds
const DAY = 60*60*24

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

type Granularity struct{
	// expiry alignment in seconds, defaults to 1 second.
	EPA  uint64
	
	// expiry round up. Round up if true, round down if false.
	EPRU bool
	
	// counter table alignment in seconds, defaults to 60*60*24 (24 hours)
	CTA  uint64
	
	// counter table round up. Round up if true, round down if false.
	// if CTA is 0, this flag is ignored and the counter-table value is rounded up.
	CTRU bool
}
func (g *Granularity) convert(exp uint64) (ept, ctt uint64) {
	ept = exp
	ctt = exp
	if g.EPA!=0 {
		if g.EPRU { ept += g.EPA-1 }
		ept -= (ept % g.EPA)
	}
	if g.CTA!=0 {
		if g.CTRU { ctt += g.CTA-1 }
		ctt -= (ctt % g.CTA)
	} else {
		ctt += DAY-1
		ctt -= (ctt % DAY)
	}
	return
}


