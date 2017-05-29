#pragma once
#ifndef __CAVALIA_DATABASE_PARTITIONING_TIME_PROFILER_H__
#define __CAVALIA_DATABASE_PARTITIONING_TIME_PROFILER_H__

#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_PARTITIONING)
#if defined(PRECISE_TIMER) 
#define INIT_PARTITIONING_TIME_PROFILER \
	access_partitioning_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) access_partitioning_stat_[i] = 0; \
	access_partitioning_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];
#else
#define INIT_PARTITIONING_TIME_PROFILER \
	access_partitioning_stat_ = new long long[kMaxThreadNum]; \
	memset(access_partitioning_stat_, 0, sizeof(access_partitioning_stat_)*kMaxThreadNum); \
	access_partitioning_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif
#define BEGIN_PARTITIONING_TIME_MEASURE(thread_id) \
	access_partitioning_timer_[thread_id].StartTimer();

#define END_PARTITIONING_TIME_MEASURE(thread_id) \
	access_partitioning_timer_[thread_id].EndTimer(); \
	access_partitioning_stat_[thread_id] += access_partitioning_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_PARTITIONING_TIME_PROFILER \
	long long partitioning_res = 0; \
	for (size_t i = 0; i < kMaxThreadNum; ++i) partitioning_res += access_partitioning_stat_[i]; \
	printf("********************** PARTITIONING TIME REPORT ************\n in total, partitioning time=%lld ms\n", partitioning_res / 1000 / 1000); \
	delete[] access_partitioning_timer_; \
	access_partitioning_timer_ = NULL; \
	delete[] access_partitioning_stat_; \
	access_partitioning_stat_ = NULL;

#else
#define INIT_PARTITIONING_TIME_PROFILER ;
#define BEGIN_PARTITIONING_TIME_MEASURE(thread_id) ;
#define END_PARTITIONING_TIME_MEASURE(thread_id) ;
#define REPORT_PARTITIONING_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern long long* access_partitioning_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *access_partitioning_timer_;
#else
		extern TimeMeasurer *access_partitioning_timer_;
#endif
	}
}

#endif
