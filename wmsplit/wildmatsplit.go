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


package wmsplit

import "sort"

type tradix map[string][]string

type tsort [][2]string
func (t tsort) Len() int { return len(t) }
func (t tsort) Less(i, j int) bool { return len(t[i][0])<len(t[j][0]) }
func (t tsort) Swap(i, j int) { t[i],t[j] = t[j],t[i] }
func (t tsort) radix() (r tradix){
	if len(t)==0 { return }
	r = make(tradix)
	
	sort.Sort(t)
	if t[0][0]=="" {
		s := make([]string,len(t))
		for i,e := range t { s[i]=e[0] }
		r[""] = s
		return
	}
	for n,elem := range t {
		for i:=0 ; i<n ; i++ {
			a := t[i][0]
			b := elem[0]
			if a==b[:len(a)]
			
		}
	}
}

func splitRange(s string) [2]string{
	first,last := -1,len(s)
	for i,b := range []byte(s) {
		switch b{
		case '*','?':
			if first<0 { first = 0 }
			last = i
		}
	}
	if first<0 { return [2]string{s,""} }
	return [2]string{s[:first],s[last+1:]}
}

