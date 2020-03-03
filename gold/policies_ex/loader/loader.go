/*
Copyright (c) 2020 Simon Schmidt

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


package loader

import "github.com/maxymania/fastnntp-polyglot/gold"
import "github.com/maxymania/fastnntp-polyglot/gold/policies_ex"

type LayerElemCfg struct{
	Perform  policies_ex.MatcherConfig                      `inn:"where!"`
	ExpiresAfter int                                        `inn:"expire-after"`
}

func (l *LayerElemCfg) CreateLayerElement() (elem policies_ex.LayerElement) {
	matcher := new(policies_ex.Matcher)
	
	l.Perform.Build(matcher)
	
	elem.Criteria = matcher
	
	elem.ExpireDays = l.ExpiresAfter
	
	return
}

type LayerCfg struct{
	PerformAll bool `inn:"incremental"`
	Elements   []LayerElemCfg `inn:"@element!"`
}


func (l *LayerCfg) CreateLayer(inner gold.PostingPolicyLite) *policies_ex.Layer {
	lay := new(policies_ex.Layer)
	lay.Inner = inner
	lay.PerformAll = l.PerformAll
	lay.Element = make([]policies_ex.LayerElement,len(l.Elements))
	for i,le := range l.Elements {
		lay.Element[i] = le.CreateLayerElement()
	}
	return lay
}


