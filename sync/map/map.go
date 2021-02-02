// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)


// Map 是一个 map[interface{}]interface{}，并且它是线程安全的。无需
// 任何的锁或同步。Load，Store和Delete分摊运行时间。这个Map是应对特殊场景而
// 应用的，大部分场景应该使用普通的go map和一个额外的锁或同步器。能得到更好
// 类型安全和更好的维护其他的不变量和map的内容。
//
// Map 有两种常用的场景：1. 当某一个key的entry对象只被写一次，但是读取确
// 非常多次（读多写少场景），像值增长的缓存。2. 当多个协成对多个不相关的key
// 的集合，进行读，写，覆盖entries对象。这两种场景，使用此Map能减少锁的抢占，
// 相比于有额外一个读写锁或锁的普通map。
//
// 未初始化的Map是空的，并且已经可以被使用。一旦被使用之后的Map，禁止再次拷贝。
type Map struct {
	// 锁
	mu Mutex

	// read 包含一部分map的内容。这部分内容是可以线程
	// 安全的访问的，这也是为什么性能更好的原因。此字段
	// 可以被线程安全加载，但是假如覆盖的话被拒获取 mu 锁。
	// 存储在read里面的Entries对象可以并行的被更新，而且无
	// 需获取 mu 锁，但是更新一个之前删除的entry对象时，需要
	// 获取锁拷贝entry到dirty，并取消删除。
	read atomic.Value // 这是个 readOnly 对象

	// dirty 包含一部分map的内容，这部分内容是需要通过获
	// 取 mu 来操作的。确保 dirty map 能被快速的提升成
	// 为 read map。其也包含所有在 read map里未删除的
	// entries对象。
	// 删除的entries不会被保存在 dirty map。一个在clean
	// map删除的entry，必须取消删除，在一个新的数据存储进
	// 来之前添加到 dirty map。
	// 如果 dirty map 是nil, 那样写一个写操作会初始化它，
	// 通过拷贝一份clean map的副本，并去除已经不新鲜的数据。
	dirty map[interface{}]*entry

	// misses 记录多少次加载，当 read map 上一次更新并需要
	// 锁住 mu 去确认是否操作的key是否存在。
	// 当足够多的 misses 发生之后，以至于覆盖掉了拷贝
	// dirty map的消耗成本。 dirty map 将被提升为 read
	// map， 并且下一个存储到map的操作，将生成一个新的
	// dirty 副本。
	misses int
}

// readOnly is an immutable struct stored atomically in the Map.read field.
// readOnly 是一个不可改变的结构体来保存原子性的 Map.read 字段。
type readOnly struct {
	m       map[interface{}]*entry // 读map
	amended bool // true 当 dirty map 有一些key不在 m 里
}

// expunged 是一个随意的指针，标记entries已经在dirty map被删除。
var expunged = unsafe.Pointer(new(interface{}))

// entry 是一个map里对于某个key的槽
type entry struct {

	// p 指向存储的interface类型数据。
	// 如果 p == nil，则 entry 已经被删除了并且m.dirty == nil。
	// 如果 p == expunged, entry 已经被删除，m.dirty != nil，并且entry已经在m.dirty不存在了。
	// 否则，entry 是合法，并且保存在 m.read.m[key]。假如 m.dirty != nil, m.dirty[key]里也存在。
	//
	// 一个 entry 能被删除，原子性的用nil来替换掉原本数据。当m.dirty被创建，它将自动的用 expunged
	// 替换掉nil，并保持m.dirty[key]未设置。
	//
	// 一个 entry 的相关联值可以被原子操作替换进行更新，前提 p != expunged。如果 p == expunged，
	// 这个p只能在第一个设置 m.dirty[key] = e之后更新。因此通过dirty查找到相关entry。
	p unsafe.Pointer // *interface{}
}

// 创建entry对象
func newEntry(i interface{}) *entry {
	return &entry{p: unsafe.Pointer(&i)}
}

// Load 返回存在map里key所关联的数据，或nil当现在没有数据。ok来标识value是否在map里找到。
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	// 加载 read map，并检查是否 entry 存在。
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]

	// 如果key不存在，并且m.dirty里面有read没有的key，则进行多一次查找
	if !ok && read.amended {
		m.mu.Lock()

		// 这里又从新查了一次，因为怕在抢占锁之前，dirty刚被提升到到read。
		// 因此这里又查找了一次，确保没有发生。
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			// 从dirty map加载，miss++，并查看是否需要进行dirty提升为read。
			e, ok = m.dirty[key]
			m.missLocked()
		}
		m.mu.Unlock()
	}
	if !ok {
		return nil, false
	}
	// 加载entry里的数据
	return e.load()
}

// load entry 里的数据，如果数据的指针为nil或expunged，则
// 返回未找到。否则返回指针和找到成功。
func (e *entry) load() (value interface{}, ok bool) {
	p := atomic.LoadPointer(&e.p)
	if p == nil || p == expunged {
		return nil, false
	}
	return *(*interface{})(p), true
}

// Store 值到某个key
func (m *Map) Store(key, value interface{}) {
	// 加载read，如果值已经存在，直接尝试替换entry的里面的数据指针。
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok && e.tryStore(&value) {
		return
	}

	// 加锁来处理
	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)

	// 如果key在read里面，
	if e, ok := read.m[key]; ok {
		// 替换entry value
		if e.unexpungeLocked() {
			// The entry已经之前被标记为 expunged，暗示dirty map 部位nil。并且entry不再里面。
			m.dirty[key] = e
		}
		e.storeLocked(&value)
	} else if e, ok := m.dirty[key]; ok { // key不在read里，但是在dirty里有。替换dirty里面entry数据。
		e.storeLocked(&value)
	} else {
		// 如果没有脏数据在dirty里面，如果我们初始化dirty map，并设置amended true
		if !read.amended {
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(value)
	}
	m.mu.Unlock()
}

// tryStore 存储值到entry，当entry还没有被删除。
// 如果entry已经被删除，tryStore将返回false，并且没有改变entry。
func (e *entry) tryStore(i *interface{}) bool {
	// 这里用的for，是否因为CompareAndSwapPointer可能返回为否？？
	for {
		p := atomic.LoadPointer(&e.p)
		if p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(i)) {
			return true
		}
	}
}

// unexpungeLocked 确保entry还没有被标记为 expunged，如果 entry已经被标记为 expunged。
// 它必须在释放这次的 m.mu 之前被添加到dirty map里面了。
func (e *entry) unexpungeLocked() (wasExpunged bool) {
	return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}

// storeLocked 无任何条件的保存数据到entry。
// entry 必须必须不为 expunged。
func (e *entry) storeLocked(i *interface{}) {
	atomic.StorePointer(&e.p, unsafe.Pointer(i))
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	// Avoid locking if it's a clean hit.
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		actual, loaded, ok := e.tryLoadOrStore(value)
		if ok {
			return actual, loaded
		}
	}

	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		if e.unexpungeLocked() {
			m.dirty[key] = e
		}
		actual, loaded, _ = e.tryLoadOrStore(value)
	} else if e, ok := m.dirty[key]; ok {
		actual, loaded, _ = e.tryLoadOrStore(value)
		m.missLocked()
	} else {
		if !read.amended {
			// We're adding the first new key to the dirty map.
			// Make sure it is allocated and mark the read-only map as incomplete.
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(value)
		actual, loaded = value, false
	}
	m.mu.Unlock()

	return actual, loaded
}

// tryLoadOrStore atomically loads or stores a value if the entry is not
// expunged.
//
// If the entry is expunged, tryLoadOrStore leaves the entry unchanged and
// returns with ok==false.
func (e *entry) tryLoadOrStore(i interface{}) (actual interface{}, loaded, ok bool) {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return nil, false, false
	}
	if p != nil {
		return *(*interface{})(p), true, true
	}

	// Copy the interface after the first load to make this method more amenable
	// to escape analysis: if we hit the "load" path or the entry is expunged, we
	// shouldn't bother heap-allocating.
	ic := i
	for {
		if atomic.CompareAndSwapPointer(&e.p, nil, unsafe.Pointer(&ic)) {
			return i, false, true
		}
		p = atomic.LoadPointer(&e.p)
		if p == expunged {
			return nil, false, false
		}
		if p != nil {
			return *(*interface{})(p), true, true
		}
	}
}

// Delete deletes the value for a key.
func (m *Map) Delete(key interface{}) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended {
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			delete(m.dirty, key)
		}
		m.mu.Unlock()
	}
	if ok {
		e.delete()
	}
}

func (e *entry) delete() (hadValue bool) {
	for {
		p := atomic.LoadPointer(&e.p)
		if p == nil || p == expunged {
			return false
		}
		if atomic.CompareAndSwapPointer(&e.p, p, nil) {
			return true
		}
	}
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently, Range may reflect any mapping for that key
// from any point during the Range call.
//
// Range may be O(N) with the number of elements in the map even if f returns
// false after a constant number of calls.
func (m *Map) Range(f func(key, value interface{}) bool) {
	// We need to be able to iterate over all of the keys that were already
	// present at the start of the call to Range.
	// If read.amended is false, then read.m satisfies that property without
	// requiring us to hold m.mu for a long time.
	read, _ := m.read.Load().(readOnly)
	if read.amended {
		// m.dirty contains keys not in read.m. Fortunately, Range is already O(N)
		// (assuming the caller does not break out early), so a call to Range
		// amortizes an entire copy of the map: we can promote the dirty copy
		// immediately!
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		if read.amended {
			read = readOnly{m: m.dirty}
			m.read.Store(read)
			m.dirty = nil
			m.misses = 0
		}
		m.mu.Unlock()
	}

	for k, e := range read.m {
		v, ok := e.load()
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

// m.misses++， 如果misses已经超过m.dirty的size。
// 则dirty提升为read，并dirty设置为nil, 清空misses。
func (m *Map) missLocked() {
	m.misses++
	if m.misses < len(m.dirty) {
		return
	}
	m.read.Store(readOnly{m: m.dirty})
	m.dirty = nil
	m.misses = 0
}

// 如果dirty 未初始化，从read加载数据，并初始化真个dirty map
func (m *Map) dirtyLocked() {
	if m.dirty != nil {
		return
	}

	read, _ := m.read.Load().(readOnly)
	m.dirty = make(map[interface{}]*entry, len(read.m))
	for k, e := range read.m {
		// 如果还未被删除，则拷贝到dirty。
		if !e.tryExpungeLocked() {
			m.dirty[k] = e
		}
	}
}

// 检查entry 是否为expunged。 如果现在为nil, 尝试从nil设置为expunged。
func (e *entry) tryExpungeLocked() (isExpunged bool) {
	p := atomic.LoadPointer(&e.p)
	for p == nil {
		if atomic.CompareAndSwapPointer(&e.p, nil, expunged) {
			return true
		}
		p = atomic.LoadPointer(&e.p)
	}
	return p == expunged
}
