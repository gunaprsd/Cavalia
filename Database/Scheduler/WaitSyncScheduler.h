#pragma once
#ifndef __CAVALIA_DATABASE_ACCESS_BASED_SCHEDULER_H__
#define __CAVALIA_DATABASE_ACCESS_BASED_SCHEDULER_H__

#include <TimeMeasurer.h>
#include <AllocatorHelper.h>
#include "../Redirector/IORedirector.h"
#include "../Transaction/StoredProcedure.h"
#include "../Storage/BaseStorageManager.h"
#include "../Logger/BaseLogger.h"
#include "../Profiler/BatchSyncTimeProfiler.h"
#include "../Profiler/PartitioningTimeProfiler.h"
#include "BaseScheduler.h"
#include "AccessInfo.h"

//#define ANALYZE_BATCH

namespace Cavalia{
	namespace Database{
        class ConcurrentExecutor;
		class WaitSyncScheduler : public BaseScheduler {
		public:
			WaitSyncScheduler(IORedirector *const redirector, ConcurrentExecutor* executor, const size_t &thread_count) 
			: BaseScheduler(redirector, thread_count), 
			  executor_(executor),
			  batches_(thread_count), raw_batches_(thread_count), 
			  waiting_threads_(),
			  current_batch_idx_(-1),
			  num_pre_processed_batches_(-1),
			  lock_(),
			  done_(false) {}
			virtual ~WaitSyncScheduler(){}
			virtual void Initialize(const size_t& thread_id);
            ParamBatch* GetNextBatch(const size_t& thread_id);
			void ThreadRun();
		private:
			WaitSyncScheduler(const WaitSyncScheduler&);
			WaitSyncScheduler& operator=(const WaitSyncScheduler&);
			void SynchronizeBatchExecution(const size_t& thread_id);
		protected:
            ConcurrentExecutor* executor_;
			//all raw-batches are pre-processed and stored into batches
			std::vector<std::vector<ParamBatch*>*> raw_batches_;
			std::atomic_int num_pre_processed_batches_;
			
			std::vector<std::vector<ParamBatch*>*> batches_;
			std::atomic_int current_batch_idx_;

			boost::detail::spinlock lock_;
			std::vector<size_t> waiting_threads_;
			volatile bool done_;
		};
	}
}

#endif