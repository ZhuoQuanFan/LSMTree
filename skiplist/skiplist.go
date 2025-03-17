package skiplist

import (
	"math/rand/v2"
	"sync"
	"time"
)

type SkipNode struct {
	key       string
	value     string
	forward   []*SkipNode
	timestamp int64
}

type SkipList struct {
	level    int
	head     *SkipNode
	maxLevel int
	mutex    sync.RWMutex
	size     int
}

func NewSkipNode(key, value string, level int) *SkipNode {
	return &SkipNode{
		key:       key,
		value:     value,
		forward:   make([]*SkipNode, level+1),
		timestamp: time.Now().UnixNano(),
	}
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		level:    1,
		head:     NewSkipNode("", "", maxLevel),
		maxLevel: maxLevel,
	}
}

func (sl *SkipList) randomLevel() int {
	level := 0
	for rand.Float64() < 0.5 && level < sl.maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key, value string) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	// fmt.Printf("Putting %s: %s\n", key, value)

	update := make([]*SkipNode, sl.maxLevel+1)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && current.key == key {
		current.value = value
		current.timestamp = time.Now().UnixNano()
		return
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level + 1; i <= level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	newNode := NewSkipNode(key, value, level)
	for i := 0; i <= level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	sl.size++
}

func (sl *SkipList) Get(key string) (string, bool) {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}
	current = current.forward[0]

	if current != nil && current.key == key {
		// fmt.Printf("Found %s: %s\n", key, current.value)
		return current.value, true
	}
	// fmt.Printf("Not found %s\n", key)
	return "", false
}

func (sl *SkipList) Size() int {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	return sl.size
}

func (sl *SkipList) ToMap() map[string]string {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	result := make(map[string]string)
	for current := sl.head.forward[0]; current != nil; current = current.forward[0] {
		result[current.key] = current.value
	}
	return result
}
