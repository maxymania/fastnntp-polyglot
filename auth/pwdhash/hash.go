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


/*
Password-Hash Interface.
*/
package pwdhash

import "crypto/sha256"
import "bytes"
import "errors"

type PasswordHash interface {
	CompareHashAndPassword(hashedPassword, password []byte) error
	GenerateFromPassword(password []byte, cost int) ([]byte, error)
}

var (
	ErrPasswordMismatch = errors.New("Password Mismatch")
	ErrInvalidPasswordHash = errors.New("Invalid Password Hash")
	ErrUnknownPasswordHashFunction = errors.New("Unknown Password Hash Function")
)

type plain int
func (plain) CompareHashAndPassword(hashedPassword, password []byte) error {
	if bytes.Equal(hashedPassword,password) { return nil }
	return ErrPasswordMismatch
}
func (plain) GenerateFromPassword(password []byte, cost int) ([]byte, error) {
	cop := make([]byte,len(password))
	copy(cop,password)
	return cop,nil
}
func (plain) String() string { return "plain" }

type sha2 int
func (sha2) CompareHashAndPassword(hashedPassword, password []byte) error {
	if len(hashedPassword)!=sha256.Size { return ErrInvalidPasswordHash }
	hash := sha256.Sum256(password)
	for i,b := range hash {
		if hashedPassword[i]!=b { return ErrPasswordMismatch }
	}
	return nil
}
func (sha2) GenerateFromPassword(password []byte, cost int) ([]byte, error) {
	hash := sha256.Sum256(password)
	return hash[:],nil
}
func (sha2) String() string { return "sha2" }

var algs = make(map[string]PasswordHash)

func init() {
	algs["plain"] = plain(0)
	algs["sha2"] = sha2(0)
	algs["sha256"] = sha2(0)
	algs[""] = sha2(0)
}

func Register(name string, ph PasswordHash) {
	_,ok := algs[name]
	if ok { panic("conflict: algorithm already exists: '"+name+"'") }
	algs[name] = ph
}

func DefaultPasswordHash() PasswordHash { return algs[""] }
func GetPasswordHash(name string) (PasswordHash,error) {
	ph,ok := algs[name]
	if !ok { return nil,ErrUnknownPasswordHashFunction }
	return ph,nil
}

// #
