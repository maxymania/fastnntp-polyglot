/*
MIT License

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

package blue

import "sync"

var pools = [...]sync.Pool{
	sync.Pool{New:func()interface{}{ return make([]Request, 16) }},
	sync.Pool{New:func()interface{}{ return make([]Request, 32) }},
	sync.Pool{New:func()interface{}{ return make([]Request, 64) }},
	sync.Pool{New:func()interface{}{ return make([]Request,128) }},
}

func allocra(n int) interface{} {
	if n<=16  { return pools[0].Get() }
	if n<=32  { return pools[1].Get() }
	if n<=64  { return pools[2].Get() }
	if n<=128 { return pools[3].Get() }
	return make([]Request,n)
}
func freera(ra []Request) {
	n := len(ra)
	if n<=16  { pools[0].Put(ra) ; return }
	if n<=32  { pools[1].Put(ra) ; return }
	if n<=64  { pools[2].Put(ra) ; return }
	if n<=128 { pools[3].Put(ra) ; return }
}

type Frontend struct {
	R Requester
}
func (f *Frontend) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, err error) {
	ra := allocra(len(groups)).([]Request)
	for i := range groups {
		r := &ra[i]
		r.IsRollback = false
		r.Group = groups[i]
		r.WG.Add(1)
		err = f.R.Offer(r)
		if err!=nil { return }
	}
	if cap(buf)<len(groups) {
		nums = make([]int64,len(groups))
	} else {
		nums = buf[:len(groups)]
	}
	for i := range groups {
		r := &ra[i]
		r.WG.Wait()
		err = r.Err
		nums[i] = int64(r.Number)
		if err!=nil { return }
	}
	freera(ra)
	return
}
func (f *Frontend) GroupHeadRevert(groups [][]byte, nums []int64) (err error) {
	ra := allocra(len(groups)).([]Request)
	for i := range groups {
		r := &ra[i]
		r.IsRollback = true
		r.Group = groups[i]
		r.Number = uint64(nums[i])
		r.WG.Add(1)
		err = f.R.Offer(r)
		if err!=nil { return }
	}
	for i := range groups {
		r := &ra[i]
		r.WG.Wait()
		err = r.Err
		if err!=nil { return }
	}
	freera(ra)
	return
}

