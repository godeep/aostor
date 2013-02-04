// uuid.go
package uuid

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
)

type UUID [Length]byte

const Length = 16

func NewUUID4() UUID {
	var u UUID
	b := u[:Length]
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		log.Fatal(err)
	}
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return u
}

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[:4], u[4:6], u[6:8], u[8:10],
		u[10:])
}
