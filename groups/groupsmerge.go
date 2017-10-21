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


package groups


func skipPos(low, high, count, pos int64) int64 {
	if high!=0 {
		pos = high+1
	}
	return pos
}
type GroupEntry struct {
	Low1,High1,Count1 int64
	Low2,High2,Count2 int64
}
func (g *GroupEntry) Increment() int64 {
	pos := skipPos(g.Low1,g.High1,g.Count1,0)
	pos  = skipPos(g.Low2,g.High2,g.Count2,pos)
	if pos==0 { pos++ }
	g.High2 = pos
	if g.Low2==0 { g.Low2 = pos }
	g.Count2++
	return pos
}
func (g *GroupEntry) Rollback(i int64) {
	if g.Low2==0 { return }
	if g.High2==i { g.High2-- }
	g.Count2--
	if g.Count2==0 || g.Low2==g.High2 {
		g.Low2,g.High2,g.Count2 = 0,0,0
	}
}
func (g *GroupEntry) MoveDown() {
	if g.Low2==0 { return }
	if g.Low1==0 {
		g.Low1,g.High1,g.Count1,g.Low2,g.High2,g.Count2 = g.Low2,g.High2,g.Count2,0,0,0
	} else {
		g.High1   = g.High2
		g.Count1 += g.Count2
		g.Low2,g.High2,g.Count2 = 0,0,0
	}
}
func (g *GroupEntry) UpdateDown(oldhigh,low,high,count int64) bool {
	if g.High1!=oldhigh { return false }
	g.High1 = high
	g.Low1 = low
	g.Count1 = count
	return true
}
func (g *GroupEntry) HlStats() (low,high,count int64) {
	if g.Low1!=0 {
		low = g.Low1
		high = g.High1
		count = g.Count1
	}
	if g.Low2!=0 {
		if low==0 { low = g.Low2 }
		count += g.Count2
		high = g.High2
	}
	return
}

