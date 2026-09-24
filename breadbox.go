// An implementation of a simple, general-purpose, in-process cache.
package breadbox

import (
	"container/heap"
	"errors"
	"math"
	"slices"
	"sync"
	"time"
)

var ErrNotFound = errors.New("value not found in cache")

// Configuraiton for the [Cache]
//
// Users should specify at least one of `EntryLifetime` or `MaxEntries` to
// ensure that the cache is cleaned automatically. Failure to do so will leak
// the memory of entries pushed to the cache.
type Config struct {
	// How long entries live in the cache
	//
	// If zero, then entries are never removed from the cache due to lifetime
	EntryLifetime time.Duration

	// The maximum number of entries in the cache. If len(cache) >= MaxEntries,
	// then the oldest entries in the cache will be evicted when new entries are
	// inserted.
	MaxEntries int

	// Do not reset the expiration time for an entry when it is read.
	//
	// In general, this should be left to default to `false`. However, there are
	// some rare cases where your workload may benefit from a cache where entries
	// expire in FIFO order.
	NoResetOnRead bool
}

type cacheEntry[T any] struct {
	Value     T
	ExpiresAt time.Time
}

type Cache[K comparable, V any] struct {
	mtx sync.Mutex
	// configuration information that changes how the cache behaves
	config Config
	// the actual cache lookup table
	cache cacheHeap[K, V]
}

func NewCache[K comparable, V any](cfg Config) *Cache[K, V] {
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = math.MaxInt
	}

	return &Cache[K, V]{
		config: cfg,
		cache: cacheHeap[K, V]{
			Entries: make(map[K]cacheEntry[V]),
		},
	}
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.getNoLock(key)
}

func (c *Cache[K, V]) getNoLock(key K) (V, bool) {
	now := time.Now()

	// clean out any old entries from our cache
	if c.config.EntryLifetime != 0 {
		c.cache.RemoveExpired(now)
	}

	// Check the cache. If we hit, bump the expiration time on the entry
	entry, ok := c.cache.Entries[key]
	if !ok {
		return entry.Value, false
	}

	if !c.config.NoResetOnRead {
		c.cache.UpdateTime(key, now.Add(c.config.EntryLifetime))
	}
	return entry.Value, true
}

// Get a value from the cache, or create it if it does not exist.
//
// If the init function is nil, then [ErrNotFound] will be returned if the
// lookup fails. In this case, you should prefer [Cache.Get]
func (c *Cache[K, V]) GetOrInit(key K, init func() (V, error)) (V, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// First, try a regular get from the cache. If that fails, but the caller did
	// not specify an init function, return an error.
	value, ok := c.getNoLock(key)
	if ok {
		return value, nil
	} else if init == nil {
		return value, ErrNotFound
	}

	// Cache miss: call the init function to create a new entry
	value, err := init()
	if err != nil {
		return value, err
	}

	// If the cache has grown too big, start expiring old entries
	for c.cache.Len() >= c.config.MaxEntries {
		c.cache.DropOldest()
	}

	_, ok = c.cache.Insert(key, cacheEntry[V]{
		Value:     value,
		ExpiresAt: time.Now().Add(c.config.EntryLifetime),
	})
	if !ok {
		// This should never be possible since we hold the mutex and checked above.
		panic("GetOrInit tried to generate a new entry when one exists")
	}

	return value, nil
}

func (c *Cache[K, V]) Insert(key K, value V) (V, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// First, gc any expired entries
	now := time.Now()
	if c.config.EntryLifetime != 0 {
		c.cache.RemoveExpired(now)
	}

	// If the cache has grown too big, start expiring old entries
	for c.cache.Len() >= c.config.MaxEntries {
		c.cache.DropOldest()
	}

	// Now that we have space, insert the new entry
	oldValue, ok := c.cache.Entries[key]
	c.cache.Insert(key, cacheEntry[V]{Value: value, ExpiresAt: now.Add(c.config.EntryLifetime)})
	return oldValue.Value, ok
}

// The actual data storage for the cache
//
// This custom data structure is a hybrid between a heap (allowing us to keep
// our entries in time-order with O(log(n)) updates) and a map (allowing us to
// reference values in O(1) time). Inserts require adding to both the heap and
// map, and thus take O(log(n))
type cacheHeap[K comparable, V any] struct {
	KeysHeap []K
	Entries  map[K]cacheEntry[V]
}

// Clean up expired entries
func (h *cacheHeap[K, V]) RemoveExpired(now time.Time) {
	for len(h.KeysHeap) > 0 {
		if !h.Entries[h.KeysHeap[0]].ExpiresAt.Before(now) {
			return
		}
		h.DropOldest()
	}
}

// Drop the oldest element from the heap, effectively expiring it early
func (h *cacheHeap[K, V]) DropOldest() {
	removed := heap.Pop(h).(K)
	delete(h.Entries, removed)
}

// Insert a new entry into the map, returning the old value if one existed
func (h *cacheHeap[K, V]) Insert(key K, value cacheEntry[V]) (cacheEntry[V], bool) {
	old, preexists := h.Entries[key]
	h.Entries[key] = value

	if preexists {
		h.fixByKey(key)
	} else {
		heap.Push(h, key)
	}

	return old, preexists
}

// update the expiry time for the entry with the given key
func (h *cacheHeap[K, V]) UpdateTime(key K, expiration time.Time) bool {
	value, ok := h.Entries[key]
	if !ok {
		return false
	}
	value.ExpiresAt = expiration
	h.Entries[key] = value
	return h.fixByKey(key)
}

func (h *cacheHeap[K, V]) Remove(key K) (cacheEntry[V], bool) {
	idx := slices.Index(h.KeysHeap, key)
	if idx <= 0 {
		return cacheEntry[V]{}, false
	}
	heap.Remove(h, idx)
	entry := h.Entries[key]
	delete(h.Entries, key)
	return entry, true
}

// fix the heap after updating the element with the given key
//
// returns `false` if the key cannot be found
func (h *cacheHeap[K, V]) fixByKey(key K) bool {
	idx := slices.Index(h.KeysHeap, key)
	if idx < 0 {
		return false
	}
	heap.Fix(h, idx)
	return true
}

func (h cacheHeap[K, V]) Len() int {
	return len(h.KeysHeap)
}

func (h cacheHeap[K, V]) Less(i, j int) bool {
	left := h.Entries[h.KeysHeap[i]]
	right := h.Entries[h.KeysHeap[j]]
	return left.ExpiresAt.Before(right.ExpiresAt)
}

func (h *cacheHeap[K, V]) Swap(i, j int) {
	h.KeysHeap[i], h.KeysHeap[j] = h.KeysHeap[j], h.KeysHeap[i]
}

// add x as element Len()
func (h *cacheHeap[K, V]) Push(x any) {
	h.KeysHeap = append(h.KeysHeap, x.(K))
}

// remove and return element Len() - 1.
func (h *cacheHeap[K, V]) Pop() any {
	n := len(h.KeysHeap)
	last := h.KeysHeap[n-1]
	h.KeysHeap = h.KeysHeap[0 : n-1]
	return last
}
