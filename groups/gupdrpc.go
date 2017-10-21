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

import "bufio"
import "encoding/binary"
import "fmt"
import "sync"

func writeError(w *bufio.Writer, d error) error {
	if d!=nil {
		s := d.Error()
		if len(s)>256 { s = s[:256] }
		_,e := w.WriteString(s)
		if e!=nil { return e }
	}
	return w.WriteByte(0)
}

func strDirty(str []byte) bool {
	for _,b := range str { if b==0 { return true } }
	return false
}

func readInt64(r *bufio.Reader) (int64,error) {
	sl,e := r.Peek(8) ; if e!=nil { return 0,e }
	i := int64(binary.BigEndian.Uint64(sl))
	_,e = r.Discard(8)
	return i,e
}

type GhaServer struct{
	R *bufio.Reader
	W *bufio.Writer
	Obj *GroupHeadActor
	ibuf [8]byte
	grpbuf [1<<12]byte
	grplb [16][]byte
	numlb [16]int64
}
func (g *GhaServer) Serve() error {
	for {
		e := g.serveRequest()
		if e!=nil { return e }
	}
}
func (g *GhaServer) serveRequest() error {
	var r64 int64
	var ri int
	var oldHigh,low,high,count int64
	var ok bool
	var grplist [][]byte
	var numlist []int64
	var rb byte
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return e }
	switch string(sl[:len(sl)-1]) {
		case "AdmCreateGroup":
			sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
			ri = g.Obj.AdmCreateGroup(sl[:len(sl)-1])
			e = g.W.WriteByte(byte(ri)) ; if e!=nil { return e }
			return g.W.Flush()
		case "MoveDown":
			sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
			r64,e = g.Obj.MoveDown(sl[:len(sl)-1])
			e = writeError(g.W,e) ; if e!=nil { return e }
			binary.BigEndian.PutUint64(g.ibuf[:],uint64(r64))
			_,e = g.W.Write(g.ibuf[:]) ; if e!=nil { return e }
			return g.W.Flush()
		case "GetDown":
			sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
			r64,e = g.Obj.GetDown(sl[:len(sl)-1])
			e = writeError(g.W,e) ; if e!=nil { return e }
			binary.BigEndian.PutUint64(g.ibuf[:],uint64(r64))
			_,e = g.W.Write(g.ibuf[:]) ; if e!=nil { return e }
			return g.W.Flush()
		case "UpdateDown":
			sl,e = g.R.Peek(32) ; if e!=nil { return e }
			oldHigh = int64(binary.BigEndian.Uint64(sl     ))
			low     = int64(binary.BigEndian.Uint64(sl[ 8:]))
			high    = int64(binary.BigEndian.Uint64(sl[16:]))
			count   = int64(binary.BigEndian.Uint64(sl[24:]))
			_,e = g.R.Discard(32) ; if e!=nil { return e }
			sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
			ok,e = g.Obj.UpdateDown(sl[:len(sl)-1],oldHigh,low,high,count)
			e = writeError(g.W,e) ; if e!=nil { return e }
			if ok { e = g.W.WriteByte(0xff) } else { e = g.W.WriteByte(0) }
			if e!=nil { return e }
			return g.W.Flush()
		case "GroupHeadInsert":
			rb,e = g.R.ReadByte() ; if e!=nil { return e }
			ri = int(rb)
			grplist = g.grplb[:0]
			{
				gb := g.grpbuf[:0]
				for i := 0 ; i<ri ; i++ {
					sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
					gb = append(gb,sl[:len(sl)-1]...)
					grplist = append(grplist,gb)
					gb = gb[len(gb):]
				}
			}
			numlist,e = g.Obj.GroupHeadInsert(grplist,g.numlb[:])
			e = writeError(g.W,e) ; if e!=nil { return e }
			ri = len(numlist)
			e = g.W.WriteByte(byte(ri)) ; if e!=nil { return e }
			for i := 0 ; i<ri ; i++ {
				binary.BigEndian.PutUint64(g.ibuf[:],uint64(numlist[i]))
				_,e = g.W.Write(g.ibuf[:8]) ; if e!=nil { return e }
			}
			return g.W.Flush()
		case "GroupHeadRevert":
			rb,e = g.R.ReadByte() ; if e!=nil { return e }
			ri = int(rb)
			grplist = g.grplb[:0]
			numlist = g.numlb[:0]
			{
				gb := g.grpbuf[:0]
				for i := 0 ; i<ri ; i++ {
					num,e := readInt64(g.R) ; if e!=nil { return e }
					numlist = append(numlist,num)
					sl,e = g.R.ReadSlice(0) ; if e!=nil { return e }
					gb = append(gb,sl[:len(sl)-1]...)
					grplist = append(grplist,gb)
					gb = gb[len(gb):]
				}
			}
			e = g.Obj.GroupHeadRevert(grplist,numlist)
			e = writeError(g.W,e) ; if e!=nil { return e }
			return g.W.Flush()
	}
	return fmt.Errorf("Unknown Command")
}

type GhaClient struct{
	sync.Mutex
	R *bufio.Reader
	W *bufio.Writer
	ibuf [8*4]byte
}
func (g *GhaClient) AdmCreateGroup(group []byte) int {
	g.Lock(); defer g.Unlock()
	if strDirty(group) { return 4 }
	_,e := g.W.WriteString("AdmCreateGroup\x00") ; if e!=nil { return 3 }
	_,e  = g.W.Write(group) ; if e!=nil { return 3 }
	e    = g.W.WriteByte(0) ; if e!=nil { return 3 }
	e    = g.W.Flush() ; if e!=nil { return 3 }
	//------------------------
	b,e := g.R.ReadByte() ; if e!=nil { return 3 }
	return int(b)
}
func (g *GhaClient) MoveDown(group []byte) (int64,error) {
	g.Lock(); defer g.Unlock()
	if strDirty(group) { return 0,fmt.Errorf("Group %q contains NUL-char",group) }
	verr := error(nil)
	_,e  := g.W.WriteString("MoveDown\x00") ; if e!=nil { return 0,e }
	_,e   = g.W.Write(group) ; if e!=nil { return 0,e }
	e     = g.W.WriteByte(0) ; if e!=nil { return 0,e }
	e     = g.W.Flush() ; if e!=nil { return 0,e }
	//------------------------
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return 0,e }
	if len(sl)>1 { verr = fmt.Errorf("Abroad %q",sl[:len(sl)-1]) }
	i,e := readInt64(g.R) ; if e!=nil { return 0,e }
	return i,verr
}
func (g *GhaClient) GetDown(group []byte) (int64,error) {
	g.Lock(); defer g.Unlock()
	if strDirty(group) { return 0,fmt.Errorf("Group %q contains NUL-char",group) }
	verr := error(nil)
	_,e  := g.W.WriteString("GetDown\x00") ; if e!=nil { return 0,e }
	_,e   = g.W.Write(group) ; if e!=nil { return 0,e }
	e     = g.W.WriteByte(0) ; if e!=nil { return 0,e }
	e     = g.W.Flush() ; if e!=nil { return 0,e }
	//------------------------
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return 0,e }
	if len(sl)>1 { verr = fmt.Errorf("Abroad %q",sl[:len(sl)-1]) }
	i,e := readInt64(g.R) ; if e!=nil { return 0,e }
	return i,verr
}
func (g *GhaClient) UpdateDown(group []byte,oldHigh,low,high,count int64) (bool,error) {
	g.Lock(); defer g.Unlock()
	verr := error(nil)
	if strDirty(group) { return false,fmt.Errorf("Group %q contains NUL-char",group) }
	binary.BigEndian.PutUint64(g.ibuf[ 0:],uint64(oldHigh))
	binary.BigEndian.PutUint64(g.ibuf[ 8:],uint64(low))
	binary.BigEndian.PutUint64(g.ibuf[16:],uint64(high))
	binary.BigEndian.PutUint64(g.ibuf[24:],uint64(count))
	_,e  := g.W.WriteString("UpdateDown\x00") ; if e!=nil { return false,e }
	_,e   = g.W.Write(g.ibuf[:32]) ; if e!=nil { return false,e }
	_,e   = g.W.Write(group) ; if e!=nil { return false,e }
	e     = g.W.WriteByte(0) ; if e!=nil { return false,e }
	e     = g.W.Flush() ; if e!=nil { return false,e }
	//------------------------
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return false,e }
	if len(sl)>1 { verr = fmt.Errorf("Abroad %q",sl[:len(sl)-1]) }
	b,e  := g.R.ReadByte() ; if e!=nil { return false,e }
	return b!=0,verr
}
func (g *GhaClient) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	g.Lock(); defer g.Unlock()
	verr := error(nil)
	ngr := len(groups)
	if ngr>255 { return nil,fmt.Errorf("Too many groups: %d",ngr) }
	for _,group := range groups {
		if strDirty(group) { return nil,fmt.Errorf("Group %q contains NUL-char",group) }
	}
	_,e  := g.W.WriteString("GroupHeadInsert\x00") ; if e!=nil { return nil,e }
	g.W.WriteByte(byte(ngr))
	for _,group := range groups {
		_,e = g.W.Write(group) ; if e!=nil { return nil,e }
		e = g.W.WriteByte(0) ; if e!=nil { return nil,e }
	}
	e = g.W.Flush() ; if e!=nil { return nil,e }
	//------------------------
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return nil,e }
	if len(sl)>1 { verr = fmt.Errorf("Abroad %q",sl[:len(sl)-1]) }
	b,e := g.R.ReadByte() ; if e!=nil { return nil,e }
	ri := int(b)
	
	if cap(buf)<ri {
		buf = make([]int64,ri)
	} else {
		buf = buf[:ri]
	}
	for i := 0 ; i<ri ; i++ {
		buf[i],e = readInt64(g.R) ; if e!=nil { return nil,e }
	}
	return buf,verr
}
func (g *GhaClient) GroupHeadRevert(groups [][]byte, nums []int64) error {
	g.Lock(); defer g.Unlock()
	verr := error(nil)
	ngr := len(groups)
	if ngr>255 { return fmt.Errorf("Too many groups: %d",ngr) }
	if ngr!=len(nums) { return fmt.Errorf("Length mismatch %d != %d",ngr,len(nums)) }
	for _,group := range groups {
		if strDirty(group) { return fmt.Errorf("Group %q contains NUL-char",group) }
	}
	_,e  := g.W.WriteString("GroupHeadRevert\x00") ; if e!=nil { return e }
	g.W.WriteByte(byte(ngr))
	for i,group := range groups {
		binary.BigEndian.PutUint64(g.ibuf[:8],uint64(nums[i]))
		_,e = g.W.Write(g.ibuf[:8]) ; if e!=nil { return e }
		_,e = g.W.Write(group) ; if e!=nil { return e }
		e = g.W.WriteByte(0) ; if e!=nil { return e }
	}
	e = g.W.Flush() ; if e!=nil { return e }
	//------------------------
	sl,e := g.R.ReadSlice(0) ; if e!=nil { return e }
	if len(sl)>1 { verr = fmt.Errorf("Abroad %q",sl[:len(sl)-1]) }
	return verr
}


