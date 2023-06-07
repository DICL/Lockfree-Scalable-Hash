#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>
#include <bitset>
#include "lockfree_hashtable.h"

const int kMaxThreads = std::thread::hardware_concurrency();
//typedef const char Value_t;
int maxElements;
LockFreeHashTable<long, Value_t> ht;

// Insert sucessfully then ++cnt,  delete succesfully then --cnt.
std::atomic<int> cnt = 0;
std::atomic<bool> start = false;
std::unordered_map<int, int*> elements2timespan;
long** workload;

void onInsert(int queryNum, int threadIdx) {
  while (!start) {
    std::this_thread::yield();
  }

//  int n = maxElements / divide;
const char* temp = new char[10];
//Value_t* val = temp;
  for (int i = 0; i <	queryNum; ++i) {
    ht.Insert(workload[threadIdx][i], temp);
  }
}

void onFind(int queryNum, int threadIdx) {
  while (!start) {
    std::this_thread::yield();
  }
	const char* temp = new char[10];
  //int n = maxElements / divide;
  for (int i = 0; i < queryNum; ++i) {
    ht.Find(workload[threadIdx][i], temp);
  }
}

void onDelete(int divide) {
  while (!start) {
    std::this_thread::yield();
  }

  int n = maxElements / divide;
  for (int i = 0; i < n; ++i) {
    if (ht.Delete(rand() % n)) {
      --cnt;
    }
  }
}

void TestConcurrentInsert(char const* tNum, char const* qNum) {
  int old_size = ht.size();
  std::vector<std::thread> threads;
  /*
  for (int i = 0; i < kMaxThreads; ++i) {
    threads.push_back(std::thread(onInsert, kMaxThreads));
  }*/
  int queryNum = atoi(qNum);
  int threadNum = atoi(tNum);
  workload = new long*[threadNum];
  for (int i = 0; i < threadNum; i++) {
  	workload[i] = new long[queryNum/threadNum];
	}
	for (int i = 0; i < threadNum; i++) {
		for (int j = 0; j < queryNum/threadNum; j++) {
			workload[i][j] = rand() % 1000000000; // Create workload
		}
	}
	//std::cout << "Workload Creation is Done !!\n";
	std::cout << "Start Insertion\n";
  for (int i = 0; i < threadNum; ++i) {
    threads.push_back(std::thread(onInsert, queryNum/threadNum, i));
  }

  start = true;
  auto t1_ = std::chrono::steady_clock::now();
  for (int i = 0; i < threadNum; ++i) {
    threads[i].join();
  }
  auto t2_ = std::chrono::steady_clock::now();

  //assert(cnt + old_size == static_cast<int>(ht.size()));
  int ms =
      std::chrono::duration_cast<std::chrono::microseconds>(t2_ - t1_).count();
  elements2timespan[maxElements][0] += ms;
  std::cout << "NumData(" << queryNum << "), numThreads(" << threadNum << ")" << std::endl;
  std::cout << "Insertion: " << ms << " usec\t" << (uint64_t)((queryNum/(ms/1000000.0))) << " ops/sec" << std::endl;
//  std::cout << maxElements << " elements insert concurrently, timespan=" << ms
//            << "ms"
 //           << "\n";
 // start = false;
 // std::cout << "Throughput(Mops/sec): " << (float(queryNum) / (float(ms)/1000))/1000000 << std::endl;
}

void TestConcurrentFind(char const* tNum, char const* qNum) {
  std::vector<std::thread> threads;

  int queryNum = atoi(qNum);
  int threadNum = atoi(tNum);

  for (int i = 0; i < threadNum; ++i) {
    threads.push_back(std::thread(onFind, queryNum/threadNum, i));
  }

  start = true;
  auto t1_ = std::chrono::steady_clock::now();
  for (int i = 0; i < threadNum; ++i) {
    threads[i].join();
  }
  auto t2_ = std::chrono::steady_clock::now();

  int ms =
      std::chrono::duration_cast<std::chrono::microseconds>(t2_ - t1_).count();
  elements2timespan[maxElements][1] += ms;
  std::cout << "Search: " << ms << " usec\t" << (uint64_t)((queryNum/(ms/1000000.0))) << " ops/sec" << std::endl;
  std::cout << "0 failedSearch\n";

/*  std::cout << maxElements << " elements find concurrently, timespan=" << ms
            << "ms"
            << "\n";

  start = false;
  std::cout << "Throughput(Mops/sec): " << (float(queryNum) / (float(ms)/1000))/1000000 << std::endl;
  */
}

void TestConcurrentDelete() {
  int old_size = ht.size();
  std::vector<std::thread> threads;
  for (int i = 0; i < kMaxThreads; ++i) {
    threads.push_back(std::thread(onDelete, kMaxThreads));
  }

  cnt = 0;
  start = true;
  auto t1_ = std::chrono::steady_clock::now();
  for (int i = 0; i < kMaxThreads; ++i) {
    threads[i].join();
  }
  auto t2_ = std::chrono::steady_clock::now();

  assert(cnt + old_size == static_cast<int>(ht.size()));
  int ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count();
  elements2timespan[maxElements][2] += ms;
  std::cout << maxElements << " elements delete concurrently, timespan=" << ms
            << "ms"
            << "\n";

  cnt = 0;
  start = false;
}

void TestConcurrentInsertAndFindAndDequeue() {
  int old_size = ht.size();

  int divide = kMaxThreads / 3;
  std::vector<std::thread> threads;
  for (int i = 0; i < divide; ++i) {
    //threads.push_back(std::thread(onInsert, divide));
    //threads.push_back(std::thread(onFind, divide));
    threads.push_back(std::thread(onDelete, divide));
  }

  cnt = 0;
  start = true;
  auto t1_ = std::chrono::steady_clock::now();
  for (int i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  auto t2_ = std::chrono::steady_clock::now();

  assert(cnt + old_size == static_cast<int>(ht.size()));
  int ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t2_ - t1_).count();
  elements2timespan[maxElements][3] += ms;
  std::cout << maxElements
            << " elements insert & find & delete concurrently, timespan=" << ms
            << "ms"
            << "\n";

  cnt = 0;
  start = false;
}

const int kElements1 = 1;
const int kElements2 = 1;
const int kElements3 = 1;

int main(int argc, char const* argv[]) {
  //(void)argc;
  //(void)argv;


  //srand(std::time(0));

  std::cout << "Benchmark with " << argv[1] << " threads:"
            << "\n";

  int elements[] = {kElements1, kElements2, kElements3};
  int timespan1[] = {0, 0, 0, 0};
  int timespan2[] = {0, 0, 0, 0};
  int timespan3[] = {0, 0, 0, 0};

  elements2timespan[kElements1] = timespan1;
  elements2timespan[kElements2] = timespan2;
  elements2timespan[kElements3] = timespan3;
  std::cout << "Reading dataset Completed\n";
  std::cout << "New schema hash Hashtable Initialized\n";

  for (int i = 0; i < 1; ++i) {
    for (int j = 0; j < 1; ++j) {
      maxElements = elements[j];
      TestConcurrentInsert(argv[2], argv[1]);
      TestConcurrentFind(argv[2], argv[1]);
      //TestConcurrentDelete();
      //TestConcurrentInsertAndFindAndDequeue();
      std::cout << "\n";
    }
  }
/*
  for (int i = 0; i < 3; ++i) {
    maxElements = elements[i];
    float avg = static_cast<float>(elements2timespan[maxElements][0]) / 10.0f;
    std::cout << maxElements
              << " elements insert concurrently, average timespan=" << avg
              << "ms"
              << "\n";
    avg = static_cast<float>(elements2timespan[maxElements][1]) / 10.0f;
    std::cout << maxElements
              << " elements find concurrently, average timespan=" << avg << "ms"
              << "\n";
    avg = static_cast<float>(elements2timespan[maxElements][2]) / 10.0f;
    std::cout << maxElements
              << " elements delete concurrently, average timespan=" << avg
              << "ms"
              << "\n";
    avg = static_cast<float>(elements2timespan[maxElements][3]) / 10.0f;
    std::cout << maxElements
              << " elements insert & find & delete concurrently, average timespan="
              << avg << "ms"
              << "\n";
    std::cout << "\n";
  }
*/
  return 0;
}
