#ifndef LOCKFREE_HASHTABLE_H
#define LOCKFREE_HASHTABLE_H

#include <atomic>
#include <cassert>
#include <cmath>
#include <bitset>
#include <thread>
#include <time.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include "HazardPointer/reclaimer.h"
#include "pair.h"

// The maximum bucket size equals to kSegmentSize^kMaxLevel, in this case the
// maximum bucket size is 64^4. If the load factor is 0.5, the maximum number of
// items that Hash Table contains is 64^4 * 0.5 = 2^23. You can adjust the
// following two values according to your memory size.
const int kMaxLevel = 4;
const int kSegmentSize = 64;
const size_t kMaxBucketSize = pow(kSegmentSize, kMaxLevel) * 2;

constexpr size_t kSegmentBits = 8;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentNodeSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 8;
constexpr size_t kNumProbing = 16;
constexpr size_t kNumCacheLine = 4;
constexpr size_t kNumThreshold = 4;
static const size_t kNumSlot = (1 << kSegmentBits) * kNumPairPerCacheLine + kNumProbing;


inline size_t standard(const void* _ptr, size_t _len,
		size_t _seed=static_cast<size_t>(0xc70f6907UL)) {
	return std::_Hash_bytes(_ptr, _len, _seed);
}

// Hash Table can be stored 2^power_of_2_ * kLoadFactor items.
const float kLoadFactor = 0.5;

template <typename K, typename V>
class TableReclaimer;

template <typename K, typename V, typename Hash = std::hash<K>>
class LockFreeHashTable {
  static_assert(std::is_copy_constructible_v<K>, "K requires copy constructor");
  static_assert(std::is_copy_constructible_v<V>, "V requires copy constructor");
  friend TableReclaimer<K, V>;

  struct Node;
  struct DummyNode;
  struct RegularNode;
  struct SegmentNode;
  struct Segment;

  typedef size_t HashKey;
  typedef size_t SegmentKey;
  typedef size_t BucketIndex;
  typedef size_t SegmentIndex;
  typedef std::atomic<SegmentNode*> Bucket;

 public:
  LockFreeHashTable() : power_of_2_(1), size_(0), hash_func_(Hash()) {
    // Initialize first bucket
    int level = 1;
    Segment* segments = segments_;  // Point to current segment.
    while (level++ <= kMaxLevel - 2) {
      Segment* sub_segments = NewSegments(level);
      segments[0].data.store(sub_segments, std::memory_order_release);
      segments = sub_segments;
    }

    Bucket* buckets = NewBuckets();
    segments[0].data.store(buckets, std::memory_order_release);

    SegmentNode* head = new SegmentNode(0, hash_func_, 1, 1, true);
    buckets[0].store(head, std::memory_order_release);
    head_ = head;
  }

  ~LockFreeHashTable() {
    Node* p = head_;
    while (p != nullptr) {
      Node* tmp = p;
      p = p->next.load(std::memory_order_acquire);
      tmp->Release();
    }
  }

  LockFreeHashTable(const LockFreeHashTable& other) = delete;
  LockFreeHashTable(LockFreeHashTable&& other) = delete;
  LockFreeHashTable& operator=(const LockFreeHashTable& other) = delete;
  LockFreeHashTable& operator=(LockFreeHashTable&& other) = delete;

  bool Insert(const K& key, const V& value) {
    RegularNode* new_node = new RegularNode(key, value, hash_func_);
    SegmentNode* head = GetBucketHeadByHash(new_node->segment_key);
    return InsertRegularNode(head, new_node);
  }

  bool Insert(K&& key, const V& value) {
    RegularNode* new_node = new RegularNode(std::move(key), value, hash_func_);
    SegmentNode* head = GetBucketHeadByHash(new_node->segment_key);
    return InsertRegularNode(head, new_node);
  }

  bool Insert(const K& key, V&& value) {
    RegularNode* new_node = new RegularNode(key, std::move(value), hash_func_);
    SegmentNode* head = GetBucketHeadByHash(new_node->segment_key);
    return InsertRegularNode(head, new_node);
  }

  bool Insert(K&& key, V&& value) {
    RegularNode* new_node =
        new RegularNode(std::move(key), std::move(value), hash_func_);
    SegmentNode* head = GetBucketHeadByHash(new_node->segment_key);
    return InsertRegularNode(head, new_node);
  }

  bool Delete(const K& key) {
    HashKey hash = hash_func_(key);
    SegmentNode* head = GetBucketHeadByHash(hash);
    RegularNode delete_node(key, hash_func_);
    return DeleteNode(head, &delete_node);
  }

  bool Find(const K& key, V& value) {
    HashKey hash = standard(&key, sizeof(key));
    SegmentNode* head = GetBucketHeadByHash(hash >> kShift);
    RegularNode find_node(hash, hash_func_);
    return FindNode(head, &find_node, value);
    return true;
  };

  size_t size() const { return size_.load(std::memory_order_relaxed); }

 private:
  size_t bucket_size() const {
    return 1 << power_of_2_.load(std::memory_order_relaxed);
  }

  Segment* NewSegments(int level) {
    Segment* segments = new Segment[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      segments[i].level = level;
      segments[i].data.store(nullptr, std::memory_order_release);
    }
    return segments;
  }

  Bucket* NewBuckets() {
    Bucket* buckets = new Bucket[kSegmentSize];
    for (int i = 0; i < kSegmentSize; ++i) {
      buckets[i].store(nullptr, std::memory_order_release);
    }
    return buckets;
  }

  // Initialize bucket recursively.
  SegmentNode* InitializeBucket(BucketIndex bucket_index);

  // When the table size is 2^i , a logical table bucket b contains items whose
  // keys k maintain k mod 2^i = b. When the size becomes 2^i+1, the items of
  // this bucket are split into two buckets: some remain in the bucket b, and
  // others, for which k mod 2^(i+1) == b + 2^i.
  BucketIndex GetBucketParent(BucketIndex bucket_index) const {
    //__builtin_clzl: Get number of leading zero bits.
    // Unset the MSB(most significant bit) of bucket_index;
    return (~(0x8000000000000000 >> (__builtin_clzl(bucket_index))) &
            bucket_index);
  };

  // Get the head node of bucket, if bucket not exist then return nullptr or
  // return head.
  SegmentNode* GetBucketHeadByIndex(BucketIndex bucket_index);

  // Get the head node of bucket, if bucket not exist then initialize it and
  // return head.
  SegmentNode* GetBucketHeadByHash(HashKey hash) {
    BucketIndex bucket_index = (hash & (bucket_size() - 1));
    SegmentNode* head = GetBucketHeadByIndex(bucket_index);
    if (nullptr == head) {
      head = InitializeBucket(bucket_index);
    }
    return head;
  }

  // Harris' OrderedListBasedset with Michael's hazard pointer to manage memory,
  // See also https://github.com/bhhbazinga/LockFreeLinkedList.
  bool InsertRegularNode(SegmentNode* head, RegularNode* new_node);
  bool InsertDummyNode(SegmentNode* head, SegmentNode* new_node,
                       SegmentNode** real_head);
  bool DeleteNode(SegmentNode* head, Node* delete_node);
  bool FindNode(SegmentNode* head, RegularNode* find_node, V& value) {
    Node* prev;
    Node* cur;
    HazardPointer prev_hp, cur_hp;
		bool found;
		auto& reclaimer = TableReclaimer<K, V>::GetInstance();
try_again:
    if (SearchNode(head, find_node, &prev, &cur, prev_hp, cur_hp, true)) {
			found = static_cast<SegmentNode*>(cur)->Search(find_node->hash);
			if (!found) {
				Node* temp = head;
				head = static_cast<SegmentNode*>(temp->get_next());
				goto try_again;
			}
		}
    return found;
  }


  size_t SearchItemSize(SegmentNode* head);

  // Traverse list begin with head until encounter nullptr or the first node
  // which is greater than or equals to the given search_node.
  bool SearchNode(SegmentNode* head, Node* search_node, Node** prev_ptr,
                  Node** cur_ptr, HazardPointer& prev_hp,
                  HazardPointer& cur_hp, bool isRead);

  // Compare two nodes according to their reverse_hash and the key.
  bool Less(Node* node1, Node* node2, bool isRead) const {
  	size_t local_level;
  	if (isRead) {
  		local_level = (node1->IsSegment()) ? static_cast<SegmentNode*>(node1)->r_level_.load(std::memory_order_release) :
				static_cast<SegmentNode*>(node2)->r_level_.load(std::memory_order_release);
		} else {
			local_level = (node1->IsSegment()) ? static_cast<SegmentNode*>(node1)->w_level_.load(std::memory_order_release) :
				static_cast<SegmentNode*>(node2)->w_level_.load(std::memory_order_release);
		}
		BucketIndex key_1 = node1->segment_key & ((1 << local_level) -1);
		BucketIndex key_2 = node2->segment_key & ((1 << local_level) -1);
		if (key_1 == key_2) {
			return false;
		}

    if (node1->reverse_hash != node2->reverse_hash) {
      return node1->reverse_hash < node2->reverse_hash;
    }

    if (node1->IsSegment() || node2->IsSegment()) {
      // When initialize bucket concurrently, that could happen.
      return false;
    }

    return static_cast<RegularNode*>(node1)->key <
           static_cast<RegularNode*>(node2)->key;
  }

  bool GreaterOrEquals(Node* node1, Node* node2, bool read) const {
    return !(Less(node1, node2, read));
  }

  bool Equals(Node* node1, Node* node2, bool read) const {
    return !Less(node1, node2, read) && !Less(node2, node1, read);
  }

  bool is_marked_reference(Node* next) const {
    return (reinterpret_cast<unsigned long>(next) & 0x1) == 0x1;
  }

  Node* get_marked_reference(Node* next) const {
    return reinterpret_cast<Node*>(reinterpret_cast<unsigned long>(next) | 0x1);
  }

  Node* get_unmarked_reference(Node* next) const {
    return reinterpret_cast<Node*>(reinterpret_cast<unsigned long>(next) &
                                   ~0x1);
  }

  static void OnDeleteNode(void* ptr) { delete static_cast<Node*>(ptr); }

  struct Node {
    Node(HashKey hash_, bool segment)
        : hash(hash_),
					segment_key(hash_ >> kShift),
          reverse_hash(segment ? DummyKey(segment_key) : RegularKey(segment_key)),
          next(nullptr) {}

    virtual void Release() = 0;

    virtual ~Node() {}

    HashKey Reverse(HashKey hash) const {
      return reverse8bits_[hash & 0xff] << 56 |
             reverse8bits_[(hash >> 8) & 0xff] << 48 |
             reverse8bits_[(hash >> 16) & 0xff] << 40 |
             reverse8bits_[(hash >> 24) & 0xff] << 32 |
             reverse8bits_[(hash >> 32) & 0xff] << 24 |
             reverse8bits_[(hash >> 40) & 0xff] << 16 |
             reverse8bits_[(hash >> 48) & 0xff] << 8 |
             reverse8bits_[(hash >> 56) & 0xff];
    }
    HashKey RegularKey(SegmentKey segment_key) const {
      return Reverse(segment_key | 0x8000000000000000);
    }
    HashKey DummyKey(HashKey hash) const { return Reverse(hash); }

    virtual bool IsSegment() const { return (reverse_hash & 0x1) == 0; }
    Node* get_next() const { return next.load(std::memory_order_acquire); }

    const HashKey hash;
    const SegmentKey segment_key;
    const HashKey reverse_hash;
    std::atomic<Node*> next;
  };

  // Head node of bucket
  struct DummyNode : Node {
    DummyNode(BucketIndex bucket_index) : Node(bucket_index, true) {}
    ~DummyNode() override {}

    void Release() override { delete this; }

    bool IsDummy() const override { return true; }
  };

  struct RegularNode : Node {
    RegularNode(const K& key_, const V& value_, const Hash& hash_func)
        : Node(standard(&key_, sizeof(key_)), false), key(key_), value(value_) {}
    RegularNode(const K& key_, V&& value_, const Hash& hash_func)
        : Node(standard(&key_, sizeof(key_)), false),
          key(key_),
          value(new V(std::move(value_))) {}
    RegularNode(K&& key_, const V& value_, const Hash& hash_func)
        : Node(hash_func(key_), false),
          key(std::move(key_)),
          value(new V(value_)) {}
    RegularNode(K&& key_, V&& value_, const Hash& hash_func)
        : Node(hash_func(key_), false),
          key(std::move(key_)),
          value(new V(std::move(value_))) {}

    RegularNode(const K& key_, const Hash& hash_func)
        //: Node(standard(&key_, sizeof(key_)), false), key(key_), value(nullptr) {}
				: Node(key_, false), key(key_), value(nullptr) {}

    ~RegularNode() override {
      V ptr = value.load(std::memory_order_consume);
      if (ptr != nullptr)
        delete ptr;  // If update a node, value of this node is nullptr.
    }

    void Release() override { delete this; }

    bool IsSegment() const override { return false; }

    const K key;
    std::atomic<V> value;
  };

  struct SegmentNode : Node {
  	SegmentNode(const K& key_, const Hash& hash_func, size_t w_level, size_t r_level, bool head) : Node(hash_func(key_), true) {
			w_level_ = w_level;
			r_level_ = r_level;
			head_ = head;
			for (size_t entry = 0; entry < kNumSlot; entry++) {
				_[entry].key.store(EMPTY, std::memory_order_release);
			}
		}
		SegmentNode(const K&& key_, const Hash& hash_func, size_t w_level, size_t r_level, bool head) : Node(hash_func(key_), true) {
			w_level_ = w_level;
			r_level_ = r_level;
			head_ = head;
			for (size_t entry = 0; entry < kNumSlot; entry++) {
				_[entry].key.store(EMPTY, std::memory_order_release);
			}
		}

		bool Insert(K key_, V value_, SegmentKey segment_key) {
			//auto mask = 1 << w_level_;
			long empty = EMPTY;
			long invalid = INVALID;
			size_t loc = (key_ & 0xff) * kNumPairPerCacheLine;
			/* Insert (Single CacheLine) */
			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_weak(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_weak(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			
			/* Linear Probing */
			
			loc += kNumPairPerCacheLine;
			for (size_t i = 0; i < kNumProbing; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			
			/* Double Hashing (Reverse the bucket bit) */
			
			size_t reverse_key_ = (key_ & 0xff) ^ 0xff;
			loc = reverse_key_ * kNumPairPerCacheLine;

			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			return true;
		}

		bool Insert4Split(K key_, V value_) {
			long empty = EMPTY;
			long invalid = INVALID;
			size_t loc = (key_ & 0xff) * kNumPairPerCacheLine;
			/* Insert (Single CacheLine) */
			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) { // Do not store outdated KV
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			/* Linear Probing */
			loc += kNumPairPerCacheLine;
			for (size_t i = 0; i < kNumProbing; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) { // Do not store outdated KV
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			/* Double Hashing (Reverse the bucket bit) */
			
			size_t reverse_key_ = (key_ & 0xff) ^ 0xff;
			loc = reverse_key_ * kNumPairPerCacheLine;

			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				empty = EMPTY;
				invalid = INVALID;
				if (_[loc + i].key.load(std::memory_order_relaxed) == key_) { // Do not store outdated KV
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(empty, key_)) {
					_[loc + i].value = value_;
					return false;
				} else if (_[loc + i].key.compare_exchange_strong(invalid, key_)) {
					_[loc + i].value = value_;
					return false;
				}
			}
			return true;
		}

		bool Search(K key_) {
			size_t loc = (key_ & 0xff) * kNumPairPerCacheLine;
			/* Search (Single CacheLine) */
			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				K sKey = _[loc + i].key.load(std::memory_order_relaxed);
				if (sKey == EMPTY || sKey == INVALID) {
					continue;
				} else if (sKey == key_) {
					return true;
				}
			}
			/* Linear Probing */

			loc += kNumPairPerCacheLine;
			for (size_t i = 0; i < kNumProbing; ++i) {
				K sKey = _[loc + i].key.load(std::memory_order_relaxed);
				if (sKey == EMPTY || sKey == INVALID) {
					continue;
				} else if (sKey == key_) {
					return true;
				}
			}

			/* Double Hashing (Reverse the bucket bit) */
			size_t reverse_key_ = (key_ & 0xff) ^ 0xff;
			loc = reverse_key_ * kNumPairPerCacheLine;

			for (size_t i = 0; i < kNumPairPerCacheLine; ++i) {
				K sKey = _[loc + i].key.load(std::memory_order_relaxed);
				if (sKey == EMPTY || sKey == INVALID) {
					continue;
				} else if (sKey == key_) {
					return true;
				}
			}
			return false;
		}

		void Release() override { delete this; }
		
		bool IsSegment() const override { return true; }

		~SegmentNode() override {}
		std::atomic<size_t> w_level_;
		std::atomic<size_t> r_level_;
		std::atomic<bool> head_;
		Pair _[kNumSlot];
	};

  struct Segment {
    Segment() : level(1), data(nullptr) {}
    explicit Segment(int level_) : level(level_), data(nullptr) {}

    Bucket* get_sub_buckets() const {
      return static_cast<Bucket*>(data.load(std::memory_order_consume));
    }

    Segment* get_sub_segments() const {
      return static_cast<Segment*>(data.load(std::memory_order_consume));
    }

    ~Segment() {
      void* ptr = data.load(std::memory_order_consume);
      if (nullptr == ptr) return;

      if (level == kMaxLevel - 1) {
        Bucket* buckets = static_cast<Bucket*>(ptr);
        delete[] buckets;
      } else {
        Segment* sub_segments = static_cast<Segment*>(ptr);
        delete[] sub_segments;
      }
    }

    int level;                // Level of segment.
    std::atomic<void*> data;  // If level == kMaxLevel then data point to
                              // buckets else data point to segments.
  };

  std::atomic<size_t> power_of_2_;   // Bucket size == 2^power_of_2_.
  std::atomic<size_t> size_;         // Item size.
  Hash hash_func_;                   // Hash function.
  Segment segments_[kSegmentSize];   // Top level sengments.
  static size_t reverse8bits_[256];  // Lookup table for reverse bits quickly.
  SegmentNode* head_;                  // Head of linkedlist.
  static Reclaimer::HazardPointerList global_hp_list_;
};

template <typename K, typename V, typename Hash>
Reclaimer::HazardPointerList LockFreeHashTable<K, V, Hash>::global_hp_list_;

template <typename K, typename V>
class TableReclaimer : public Reclaimer {
  friend LockFreeHashTable<K, V>;

 private:
  TableReclaimer(HazardPointerList& hp_list) : Reclaimer(hp_list) {}
  ~TableReclaimer() override = default;

  static TableReclaimer<K, V>& GetInstance() {
    thread_local static TableReclaimer reclaimer(
        LockFreeHashTable<K, V>::global_hp_list_);
    return reclaimer;
  }
};

// Fast reverse bits using Lookup Table.
#define R2(n) n, n + 2 * 64, n + 1 * 64, n + 3 * 64
#define R4(n) R2(n), R2(n + 2 * 16), R2(n + 1 * 16), R2(n + 3 * 16)
#define R6(n) R4(n), R4(n + 2 * 4), R4(n + 1 * 4), R4(n + 3 * 4)
// Lookup Table that store the reverse of each 8bit number.
template <typename K, typename V, typename Hash>
size_t LockFreeHashTable<K, V, Hash>::reverse8bits_[256] = {R6(0), R6(2), R6(1),
                                                            R6(3)};

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::SegmentNode*
LockFreeHashTable<K, V, Hash>::InitializeBucket(BucketIndex bucket_index) {
  BucketIndex parent_index = GetBucketParent(bucket_index);
  SegmentNode* parent_head = GetBucketHeadByIndex(parent_index);
  if (nullptr == parent_head) {
    parent_head = InitializeBucket(parent_index);
  }

  int level = 1;
  Segment* segments = segments_;  // Point to current segment.
  while (level++ <= kMaxLevel - 2) {
    Segment& cur_segment =
        segments[(bucket_index / static_cast<SegmentIndex>(pow(
                                     kSegmentSize, kMaxLevel - level + 1))) %
                 kSegmentSize];
    Segment* sub_segments = cur_segment.get_sub_segments();
    if (nullptr == sub_segments) {
      // Try allocate segments.
      sub_segments = NewSegments(level);
      void* expected = nullptr;
      if (!cur_segment.data.compare_exchange_strong(
              expected, sub_segments, std::memory_order_release)) {
        delete[] sub_segments;
        sub_segments = static_cast<Segment*>(expected);
      }
    }
    segments = sub_segments;
  }

  Segment& cur_segment = segments[(bucket_index / kSegmentSize) % kSegmentSize];
  Bucket* buckets = cur_segment.get_sub_buckets();
  if (nullptr == buckets) {
    // Try allocate buckets.
    void* expected = nullptr;
    buckets = NewBuckets();
    if (!cur_segment.data.compare_exchange_strong(expected, buckets,
                                                  std::memory_order_release)) {
      delete[] buckets;
      buckets = static_cast<Bucket*>(expected);
    }
  }

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  SegmentNode* head = bucket.load(std::memory_order_consume);

  Node* prev;
  Node* cur;
  HazardPointer prev_hp, cur_hp;
  RegularNode* temp_head = new RegularNode(bucket_index << kShift, hash_func_);
  if (SearchNode(parent_head, temp_head, &prev, &cur, prev_hp, cur_hp, false)) {
  	static_cast<SegmentNode*>(cur)->head_.store(true, std::memory_order_release);
  	head = static_cast<SegmentNode*>(cur);
  	bucket.store(head, std::memory_order_release);
  	delete temp_head;
	}

  if (nullptr == head) {
    // Try allocate dummy head.
    head = new SegmentNode(bucket_index << kShift, hash_func_,1, 1, true);
    SegmentNode* real_head;  // If insert failed, real_head is the head of bucket.
    if (InsertDummyNode(parent_head, head, &real_head)) {
      // Dummy head must be inserted into the list before storing into bucket.
      bucket.store(head, std::memory_order_release);
    } else {
      delete head;
      head = real_head;
    }
  }
  return head;
}

template <typename K, typename V, typename Hash>
typename LockFreeHashTable<K, V, Hash>::SegmentNode*
LockFreeHashTable<K, V, Hash>::GetBucketHeadByIndex(BucketIndex bucket_index) {
  int level = 1;
  const Segment* segments = segments_;
  while (level++ <= kMaxLevel - 2) {
    segments =
        segments[(bucket_index / static_cast<SegmentIndex>(pow(
                                     kSegmentSize, kMaxLevel - level + 1))) %
                 kSegmentSize]
            .get_sub_segments();
    if (nullptr == segments) return nullptr;
  }

  Bucket* buckets =
      segments[(bucket_index / kSegmentSize) % kSegmentSize].get_sub_buckets();
  if (nullptr == buckets) return nullptr;

  Bucket& bucket = buckets[bucket_index % kSegmentSize];
  return bucket.load(std::memory_order_consume);
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::InsertDummyNode(SegmentNode* parent_head,
                                                    SegmentNode* new_head,
                                                    SegmentNode** real_head) {
  Node* prev;
  Node* cur;
  HazardPointer prev_hp, cur_hp;
  do {
    if (SearchNode(parent_head, new_head, &prev, &cur, prev_hp, cur_hp, false)) {
      // The head of bucket already insert into list.
      static_cast<SegmentNode*>(cur)->head_.store(true, std::memory_order_release);
      *real_head = static_cast<SegmentNode*>(cur);
      return false;
    }
    new_head->next.store(cur, std::memory_order_release);
  } while (!prev->next.compare_exchange_weak(
      cur, new_head, std::memory_order_release, std::memory_order_relaxed));
  return true;
}

// Insert regular node into hash table, if its key is already exists in
// hash table then update it and return false else return true.

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::InsertRegularNode(SegmentNode* head,
                                                      RegularNode* new_node) {
  Node* prev;
  Node* cur;
  Node* next;
  SegmentNode* new_segment;
  HazardPointer prev_hp, cur_hp;
  bool split = false;
  auto& reclaimer = TableReclaimer<K, V>::GetInstance();
  do {
  	if (SearchNode(head, new_node, &prev, &cur, prev_hp, cur_hp, false)) {
  		V new_value = new_node->value.load(std::memory_order_consume);
  		next = cur->get_next();
  		split = static_cast<SegmentNode*>(cur)->Insert(new_node->hash, new_value, cur->segment_key);
  		if (split) {
  			size_t local_w_level = static_cast<SegmentNode*>(cur)->w_level_.load(std::memory_order_relaxed);
  			size_t local_r_level = static_cast<SegmentNode*>(cur)->r_level_.load(std::memory_order_relaxed);
  			auto mask = 1 << local_w_level;
  			new_segment = new SegmentNode(((cur->hash >> kShift) | mask) << kShift, hash_func_, local_w_level + 1, local_r_level, false);
  			new_segment->next.store(next, std::memory_order_release);
  			if (!cur->next.compare_exchange_weak(
							next, new_segment, std::memory_order_release, std::memory_order_relaxed)) {
					delete new_segment;
					InsertRegularNode(GetBucketHeadByHash(new_node->segment_key), new_node);
					return true;
				}
				static_cast<SegmentNode*>(cur)->w_level_++;
				mask = mask << 1;
				SegmentNode* prev_seg = static_cast<SegmentNode*>(cur);
				for (size_t slot = 0; slot < kNumSlot; ++slot) {
					if (prev_seg->_[slot].key.load(std::memory_order_relaxed) == (EMPTY || INVALID))
						continue;
					if (((prev_seg->_[slot].key >> kShift) & (mask - 1)) == new_segment->hash >> kShift) {
						new_segment->Insert4Split(prev_seg->_[slot].key, prev_seg->_[slot].value);
						prev_seg->_[slot].key.store(INVALID, std::memory_order_release);
					}
				}
				prev_seg->r_level_++;
				new_segment->r_level_++;
				size_t size = SearchItemSize(head);
				if (size >= kNumThreshold) {
					size_t global_level = power_of_2_.load(std::memory_order_relaxed);
					if (power_of_2_.compare_exchange_strong(global_level, global_level + 1,
																									std::memory_order_release)) {
						assert(bucket_size() <=
										kMaxBucketSize);
					}
				}
				InsertRegularNode(GetBucketHeadByHash(new_node->segment_key), new_node);
				return true;
			}
			new_node->value.store(nullptr, std::memory_order_release);
			delete new_node;
			return true;
		}
		InsertRegularNode(GetBucketHeadByHash(new_node->segment_key), new_node);
		return true;
	} while (!prev->next.compare_exchange_weak( 
			cur, new_segment, std::memory_order_release, std::memory_order_relaxed));
	return true;
}

template <typename K, typename V, typename Hash>
size_t LockFreeHashTable<K, V, Hash>::SearchItemSize(SegmentNode* head) {
	Node* prev = head;
	Node* cur = prev->get_next();
	size_t size_ = 0;
	while (true) {
		if (nullptr == cur) {break;}
		if (!(static_cast<SegmentNode*>(cur)->head_)) {
			size_++;
			prev = cur;
			cur = prev->get_next();
		}
		else {break;}
	}
	return size_;
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::SearchNode(SegmentNode* head,
                                               Node* search_node,
                                               Node** prev_ptr, Node** cur_ptr,
                                               HazardPointer& prev_hp,
                                               HazardPointer& cur_hp,
                                               bool isRead) {
  auto& reclaimer = TableReclaimer<K, V>::GetInstance();
  if (GreaterOrEquals(head, search_node, isRead)) { // Case search node is equals with head node.
  	*cur_ptr = head;
  	return Equals(head, search_node, isRead);
	}
try_again:
  Node* prev = head;
  Node* cur = prev->get_next();
  Node* next;
  while (true) {
    cur_hp.UnMark();
    cur_hp = HazardPointer(&reclaimer, cur);
    // Make sure prev is the predecessor of cur,
    // so that cur is properly marked as hazard.
    if (prev->get_next() != cur) goto try_again;

    if (nullptr == cur) {
      *prev_ptr = prev;
      *cur_ptr = cur;
      return false;
    }
    next = cur->get_next();
    if (is_marked_reference(next)) {
      if (!prev->next.compare_exchange_strong(cur,
                                              get_unmarked_reference(next)))
        goto try_again;

      reclaimer.ReclaimLater(cur, LockFreeHashTable<K, V, Hash>::OnDeleteNode);
      reclaimer.ReclaimNoHazardPointer();
      //size_.fetch_sub(1, std::memory_order_relaxed);
      cur = get_unmarked_reference(next);
    } else {
      if (prev->get_next() != cur) goto try_again;

      // Can not get copy_cur after above invocation,
      // because prev may not be the predecessor of cur at this point.
      if (GreaterOrEquals(cur, search_node, isRead)) {
        *prev_ptr = prev;
        *cur_ptr = cur;
        return Equals(cur, search_node, isRead);
      }

      // Swap cur_hp and prev_hp.
      HazardPointer tmp = std::move(cur_hp);
      cur_hp = std::move(prev_hp);
      prev_hp = std::move(tmp);

      prev = cur;
      cur = next;
    }
  };

  assert(false);
  return false;
}

template <typename K, typename V, typename Hash>
bool LockFreeHashTable<K, V, Hash>::DeleteNode(SegmentNode* head,
                                               Node* delete_node) {
  Node* prev;
  Node* cur;
  Node* next;
  HazardPointer prev_hp, cur_hp;
  do {
    do {
      if (!SearchNode(head, delete_node, &prev, &cur, prev_hp, cur_hp, false)) {
        return false;
      }
      next = cur->get_next();
    } while (is_marked_reference(next));
    // Logically delete cur by marking cur->next.
  } while (!cur->next.compare_exchange_weak(next, get_marked_reference(next),
                                            std::memory_order_release,
                                            std::memory_order_relaxed));

  if (prev->next.compare_exchange_strong(cur, next,
                                         std::memory_order_release)) {
    //size_.fetch_sub(1, std::memory_order_relaxed);
    auto& reclaimer = TableReclaimer<K, V>::GetInstance();
    reclaimer.ReclaimLater(cur, LockFreeHashTable<K, V, Hash>::OnDeleteNode);
    reclaimer.ReclaimNoHazardPointer();
  } else {
    prev_hp.UnMark();
    cur_hp.UnMark();
    SearchNode(head, delete_node, &prev, &cur, prev_hp, cur_hp, false);
  }

  return true;
}
#endif  // LOCKFREE_HASHTABLE_H
