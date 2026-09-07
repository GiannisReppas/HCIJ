#ifndef _DEF_H_
#define _DEF_H_

#include <getopt.h>
#include <immintrin.h>
#include <unistd.h>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <algorithm>
#include <span>
#include <fstream>
#include <vector>
#include <chrono>
#include <numa.h>
#include <cinttypes>
#include <oneapi/tbb/task_arena.h>
#include <oneapi/tbb/parallel_sort.h>
#include <omp.h>

#define OPT_FS_BARRIER 2500

#define WORKLOAD_COUNT 0
#define XOR            1
#define SIMD_XOR       2

#define OS          10
#define CLOSEST     11
#define OPPOSITE    12

typedef uint64_t Timestamp;

class Timer
{
private:
	using Clock = std::chrono::high_resolution_clock;
	Clock::time_point start_time, stop_time;

public:
	Timer()
	{
		start();
	}

	void start()
	{
		start_time = Clock::now();
	}

	double getElapsedTimeInSeconds()
	{
		return std::chrono::duration<double>(stop_time - start_time).count();
	}

	double stop()
	{
		stop_time = Clock::now();
		return getElapsedTimeInSeconds();
	}
};

#endif
