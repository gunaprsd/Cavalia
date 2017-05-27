#pragma once
#ifndef __CAVALIA_DATABASE_ACCESS_BASED_SCHEDULER_H__
#define __CAVALIA_DATABASE_ACCESS_BASED_SCHEDULER_H__

#include <TimeMeasurer.h>
#include <AllocatorHelper.h>
#include "../Redirector/IORedirector.h"
#include "../Transaction/StoredProcedure.h"
#include "../Storage/BaseStorageManager.h"
#include "../Logger/BaseLogger.h"
#include "BaseScheduler.h"
#include <vector>

namespace Cavalia{
	namespace Database{
        class ConcurrentExecutor;
		class AccessBasedScheduler : public BaseScheduler {
		public:
			AccessBasedScheduler(IORedirector *const redirector, ConcurrentExecutor* executor, const size_t &thread_count) : BaseScheduler(redirector, thread_count), executor_(executor) {}
			virtual ~AccessBasedScheduler(){}
			virtual void Initialize(const size_t& thread_id);
            virtual ParamBatch* GetNextBatch(const size_t& thread_id);
		private:
			AccessBasedScheduler(const AccessBasedScheduler&);
			AccessBasedScheduler& operator=(const AccessBasedScheduler&);
		protected:
            ConcurrentExecutor* executor_;
			//divide the batch from [start, start + numCores) and add them to corresponding workers
			void clusterAddBatches(std::vector<ParamBatch*>* input_batches, size_t start);
		};
	}


}

#endif