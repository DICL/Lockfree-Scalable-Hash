#ifndef UTIL_PAIR_H_
#define UTIL_PAIR_H_

#include <cstdlib>
#include <shared_mutex>

typedef std::atomic<long> Key_t;
typedef const char* Value_t;

long EMPTY = -3;
long INVALID = -1;
const Key_t SENTINEL = -2;

const Value_t NONE = 0x0;

struct Pair {
  Key_t key;
  Value_t value;

  //Pair(void)
  //: key{INVALID} { }
	Pair(void) {
		key.store(INVALID, std::memory_order_release);
	}

  //Pair(Key_t _key, Value_t _value)
  //: key{_key}, value{_value} { }
	Pair(Key_t _key, Value_t _value) {
		key.store(_key, std::memory_order_release);
		value = _value;
	}

  Pair& operator=(const Pair& other) {
    //key = other.key;
    key = other.key.load(std::memory_order_acquire);
    value = other.value;
    return *this;
  }

  void* operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

#endif // UTIL_PAIR_H_
  
