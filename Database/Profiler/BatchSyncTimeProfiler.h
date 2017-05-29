#pragma once
#ifndef __CAVALIA_DATABASE_BATCH_SYNC_TIME_PROFILER_H__
#define __CAVALIA_DATABASE_BATCH_SYNC_TIME_PROFILER_H__

#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_BATCH_SYNC)
#if defined(PRECISE_TIMER) 
#define INIT_BATCH_SYNC_TIME_PROFILER \
	access_batch_sync_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) access_batch_sync_stat_[i] = 0; \
	access_batch_sync_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];
#else
#define INIT_BATCH_SYNC_TIME_PROFILER \
	access_batch_sync_stat_ = new long long[kMaxThreadNum]; \
	memset(access_batch_sync_stat_, 0, sizeof(access_batch_sync_stat_)*kMaxThreadNum); \
	access_batch_sync_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif
#define BEGIN_BATCH_SYNC_TIME_MEASURE(thread_id) \
	access_batch_sync_timer_[thread_id].StartTimer();

#define END_BATCH_SYNC_TIME_MEASURE(thread_id) \
	access_batch_sync_timer_[thread_id].EndTimer(); \
	access_batch_sync_stat_[thread_id] += access_batch_sync_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_BATCH_SYNC_TIME_PROFILER \
	long long res = 0; \
	for (size_t i = 0; i < kMaxThreadNum; ++i) res += access_batch_sync_stat_[i]; \
	printf("********************** BATCH SYNC TIME REPORT ************\n in total, batch synchronization time=%lld ms\n", res / 1000 / 1000); \
	delete[] access_batch_sync_timer_; \
	access_batch_sync_timer_ = NULL; \
	delete[] access_batch_sync_stat_; \
	access_batch_sync_stat_ = NULL;

#else
#define INIT_BATCH_SYNC_TIME_PROFILER ;
#define BEGIN_BATCH_SYNC_TIME_MEASURE(thread_id) ;
#define END_BATCH_SYNC_TIME_MEASURE(thread_id) ;
#define REPORT_BATCH_SYNC_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern long long* access_batch_sync_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *access_batch_sync_timer_;
#else
		extern TimeMeasurer *access_batch_sync_timer_;
#endif
	}
}

#endif
