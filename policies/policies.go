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


package policies

import "compress/flate"
import "bytes"
import "time"
import "fmt"

type DEFLATE struct{}
type DeflateFunction func(d DEFLATE,data []byte) []byte
func (d DeflateFunction) Def() DeflateFunction {
	if d==nil { return NoDeflate }
	return d
}

func STDDeflate(d DEFLATE,data []byte) []byte {
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,-1)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}
func NoDeflate(d DEFLATE,data []byte) []byte {
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,0)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

func NewDeflateFunction(level int) DeflateFunction {
	if level<  -2 || 9<level { panic(fmt.Errorf("Invalid level: %d",level)) }
	return func(d DEFLATE,data []byte) []byte {
		buf := new(bytes.Buffer)
		w,e := flate.NewWriter(buf,level)
		if e!=nil { panic(e) }
		w.Write(data)
		w.Close()
		return buf.Bytes()
	}
}

type PostingDecision struct{
	ExpireAt       time.Time
	CompressXover  DeflateFunction
	CompressHeader DeflateFunction
	CompressBody   DeflateFunction
}

type PostingPolicy interface{
	Decide(groups [][]byte,lines, length int64) PostingDecision
}

type DefaultPostingPolicy struct{}
func (d DefaultPostingPolicy) Decide(groups [][]byte,lines, length int64) PostingDecision {
	exp := time.Now().UTC().AddDate(0,0,1000) // One thousand days!
	return PostingDecision{ExpireAt:exp}
}

var vDefaultPostingPolicy PostingPolicy = DefaultPostingPolicy{}

func Def(p PostingPolicy) PostingPolicy {
	if p==nil { return vDefaultPostingPolicy }
	return p
}
