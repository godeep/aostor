package bytrie

import (
// "sort"
)

// A 'map' structure, that can hold []byte objects.
// For any one []byte instance, it is either in the set or not.
type Trie struct {
	// This value can be nil if the []byte is just a prefix to a longer element.
	value    []byte
	children map[byte]*Trie
}

func New() *Trie {
	t := new(Trie)
	t.children = make(map[byte]*Trie)
	return t
}

// Returns the value of `key` in the map.
func (self *Trie) Get(key []byte) []byte {
	if len(key) == 0 {
		return self.value
	}
	child, ok := self.children[key[0]]
	if !ok {
		return nil
	}
	return child.Get(key[1:])
}

// Add `key` to the map, with `value` as value.
// If `key` is already a member, this overwrites existing value
func (self *Trie) Set(key []byte, value []byte) {
	if len(key) == 0 {
		self.value = value
		return
	}
	child, ok := self.children[key[0]]
	if !ok {
		child = New()
		self.children[key[0]] = child
	}
	child.Set(key[1:], value)
}

// Remove `key` from the map.
// If there are no instances of `key`, this does nothing.
// Return value.
func (self *Trie) Delete(key string) (value []byte) {
	if len(key) == 0 {
		value := self.value
		self.value = nil
		return value
	}
	child, ok := self.children[key[0]]
	if ok {
		if value := child.Delete(key[1:]); value == nil {
			// the child reports it is empty, hence can be deleted
			self.children[key[0]] = nil
		}
	}
	return nil
}

// The following ugliness is a wrapper for bytes,
// providing the necessary Less() method in order to sort them.
type lessByte struct {
	val byte
}

// Retrieve all values of the map, with keys starting with prefix, unordered.
func (self *Trie) Members(prefix []byte) (els [][]byte) {
	//we have to search for the last common prefix
	if prefix != nil && len(prefix) > 0 {
		if child, ok := self.children[prefix[0]]; ok {
			return child.Members(prefix[1:])
		}
		return
	}

	//leaf node?
	if self.children == nil || len(self.children) == 0 {
		els = make([][]byte, 1, 1)
		els[0] = self.value
		return
	}

	//walk and return ALL leaf nodes originating from here!
	els = make([][]byte, 0, len(self.children))
	for _, child := range self.children {
		for _, childelt := range child.Members(nil) {
			els = append(els, childelt)
		}
	}

	return
}
