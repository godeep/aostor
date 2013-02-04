package bytrie

import (
	"encoding/base64"
	"fmt"
	"github.com/tgulacsi/aostor/uuid"
	"testing"
)

func TestTrie(t *testing.T) {
	trie := New()
	key := make([]byte, 16)
	value := make([]byte, 16)
	members := make([][]byte, 0, 1)
	for i := 0; i < 1000; i++ {
		key = uuid.NewRandom()
		copy(value, key)
		value[15] = 0
		t.Logf("trie[%s] = %s", b64(key), b64(value))
		trie.Set(key, value)
		if !equal(trie.Get(key), value) {
			t.Logf("trie[%s]: %s != %s", b64(key), b64(trie.Get(key)), b64(value))
			t.FailNow()
		}

		ok := false
		value_s := b64(value)
		members = members[:0]
		members = trie.Members(key[:1])
		members_s := make([]string, len(members))
		for j, buf := range members {
			members_s[j] = b64(buf)
			if !ok && b64(buf) == value_s {
				ok = true
			}
		}
		if members == nil || len(members) < 1 {
			t.FailNow()
		}
		t.Logf("trie[%s]: %s", b64(key), members_s)
		if !ok {
			t.FailNow()
		}
	}

}

func b64(b []byte) string {
	return base64.URLEncoding.EncodeToString(b[0:])
}

func BenchmarkTrie(b *testing.B) {
	trie := New()
	key := make([]byte, 16)
	value := make([]byte, 16)
	var i int
	for i = 0; i < b.N; i++ {
		b.StopTimer()
		key = uuid.NewRandom()
		copy(value, key)
		value[15] = 0
		// b.Logf("trie[%x] = %x", key, value)
		b.StartTimer()
		trie.Set(key, value)
		b.SetBytes(32)
		if !equal(trie.Get(key), value) {
			b.Logf("trie[%x]: %x != %x", key, trie.Get(key), value)
			b.FailNow()
		}
	}
	b.Logf("N=%d", i)

}

func equal(a, b []byte) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		fmt.Printf("a or b is nil")
		return false
	}
	if len(a) != len(b) {
		fmt.Printf("length mismatch")
		return false
	}
	n := len(a)
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			fmt.Printf("byte mismatch at %d", i)
			return false
		}
	}
	return true
}
