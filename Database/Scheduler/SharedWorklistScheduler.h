#pragma once
#ifndef __CAVALIA_DATABASE_SHARED_WORKLIST_SCHEDULER_H__
#define __CAVALIA_DATABASE_SHARED_WORKLIST_SCHEDULER_H__

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
#include <vector>
#include <cmath>

#define INITIAL_PRE_PROCESS_BATCH_COUNT 5
#define MAX_ATOMIC_BATCH_SIZE 250
//#define ANALYZE_BATCH

namespace Cavalia{
	namespace Database{
        class ConcurrentExecutor;
        
        struct SimpleConcurrentWorklist
        {
            std::vector<ParamBatch*> queue_;
            std::atomic<int> next_idx_;

            SimpleConcurrentWorklist() : queue_(), next_idx_(0) {}

            void Add(ParamBatch* item) {
                queue_.push_back(item);
            }
            
            ParamBatch* GetNext() {
                int local_next_idx = next_idx_.fetch_add(1);
                if(local_next_idx < queue_.size()) {
                    return queue_[local_next_idx];
                } else {
                    return NULL;
                }
            }
        };
        
        /*
        ** Class SharedWorklistScheduler:
        ** ------------------------------
        ** In this scheduler, every super-batch is divided into several atomic-batches which are then
        ** maintained in a shared worklist and then allotted to workers dynamically. The hope is that 
        ** we do not pay much for synchronization here. 
        ** 
        ** A more advanced scheduling mechanism is possible in which we do not wait for every worker to
        ** finish processing a super-batch. But, in that case we have to be careful to not use any 
        ** contention-based optimizations such as performing concurrency control only on subset of data 
        ** items that are contentious.
        */
		class SharedWorklistScheduler : public BaseScheduler {
		public:
			SharedWorklistScheduler(IORedirector *const redirector, ConcurrentExecutor* executor, const size_t &thread_count) 
			: BaseScheduler(redirector, thread_count), 
			  executor_(executor),
              raw_batches_(thread_count), 
			  batches_(), 
			  waiting_threads_(),
			  current_batch_idx_(0),
			  lock_(),
			  done_(false) {}
			virtual ~SharedWorklistScheduler(){}
			virtual void Initialize(const size_t& thread_id);
            ParamBatch* GetNextBatch(const size_t& thread_id);
			void ThreadRun();
		private:
			SharedWorklistScheduler(const SharedWorklistScheduler&);
			SharedWorklistScheduler& operator=(const SharedWorklistScheduler&);
			void SynchronizeBatchExecution(const size_t& thread_id);
            SimpleConcurrentWorklist* DoSimplePartition(int batch_idx);
            SimpleConcurrentWorklist* DoDataBasedPartition(int batch_idx);

		protected:
            ConcurrentExecutor* executor_;
			//all raw-batches are pre-processed and stored into batches
			std::vector<std::vector<ParamBatch*>*> raw_batches_;

			std::vector<SimpleConcurrentWorklist*> batches_;
			std::atomic_int current_batch_idx_;

			boost::detail::spinlock lock_;
			std::vector<size_t> waiting_threads_;
			volatile bool done_;

		};

        bool compare_degree(BatchAccessInfo* info1, BatchAccessInfo* info2);
	}
}

#endif