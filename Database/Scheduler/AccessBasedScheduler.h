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
#include <vector>
#include <atomic>
#include <unordered_map>
#include <unordered_set>

#define INITIAL_PRE_PROCESS_BATCH_COUNT 5
#define MAX_CLUSTER_SIZE 250;
//#define ANALYZE_BATCH
#define SHARED_WORKLIST

namespace Cavalia{
	namespace Database{
        class ConcurrentExecutor;

		//This object contains information about each data item in current batch
		struct BatchAccessInfo
		{
			//data hash
			int64_t hash_;
			//for optimizations
			bool has_writes_; 
			//txn clusters in current batch that access data
			std::unordered_set<TxnParam*> txns_;
			//cached information about concurrent access
			bool avoid_cc_; 

			BatchAccessInfo(int64_t hash) : txns_() {
				hash_ = hash;
				has_writes_ = false;
				avoid_cc_ = true;
			}

			void AddTransaction(TxnParam* txn) {
				txns_.insert(txn);
				if(txns_.size() > 1) {
					avoid_cc_ = false;
				}
			}

			void RemoveTransaction(TxnParam* txn) {
				txns_.erase(txn);
				if(txns_.size() <= 1) {
					avoid_cc_ = true;
				}
			}
		};

		struct ReadWriteSet 
		{
			//map for lookup of items
			std::unordered_map<int64_t, BatchAccessInfo*> lookup_;
			
			ReadWriteSet() : lookup_() {}

			bool CanAvoidConcurrencyControl(int64_t hash) {
				auto info = lookup_[hash];
				if(info == NULL) {
					return false;
				}
				return info->avoid_cc_;
			}

			BatchAccessInfo* GetOrAdd(int64_t hash) {
				auto info = lookup_[hash];
				if(info == NULL) {
					info = new BatchAccessInfo(hash);
					lookup_[hash] = info;
				}
				return info;
			}
			
			BatchAccessInfo* Get(int64_t hash) {
				auto info = lookup_[hash];
				return info;
			}

			void Add(int64_t hash, BatchAccessInfo* info) {
				lookup_[hash] = info;
			}
		};

		// struct ClusterInfo
		// {
		// 	TxnParam* owner_;
		// 	ClusterInfo();
		// 	void MergeClusters(TxnParam* txn);
		// 	size_t GetSize();
		// };

		

		// struct TransactionData {
		// 	//stores the number of clusters in the list
		// 	int num_clusters_;
		// 	//caches the items stores in the cluster
		// 	std::unordered_set<BatchAccessInfo*> cached_items_;
		// 	//owner node
		// 	TxnParam* owner_;
		// 	//link to next node
		// 	TxnParam* next_;
		// 	//link to last node
		// 	TxnParam* last_;

		// 	TransactionNodeData(TxnParam* owner) : size_(1), cached_items_(), next_(NULL), owner_(owner), last_(owner) {}
		// 	void merge(TxnParam* param);
		// 	bool can_merge(TxnParam* param);
		// };

		class AccessBasedScheduler : public BaseScheduler {
		public:
			AccessBasedScheduler(IORedirector *const redirector, ConcurrentExecutor* executor, const size_t &thread_count) 
			: BaseScheduler(redirector, thread_count), 
			  executor_(executor),
			  batches_(thread_count), raw_batches_(thread_count), 
			  waiting_threads_(),
			  current_batch_idx_(-1),
			  num_pre_processed_batches_(-1),
			  lock_(),
			  done_(false) {}
			virtual ~AccessBasedScheduler(){}
			virtual void Initialize(const size_t& thread_id);
            ParamBatch* GetNextBatch(const size_t& thread_id);
			void ThreadRun();
		private:
			AccessBasedScheduler(const AccessBasedScheduler&);
			AccessBasedScheduler& operator=(const AccessBasedScheduler&);
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