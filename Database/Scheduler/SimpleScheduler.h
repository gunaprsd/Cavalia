#pragma once
#ifndef __CAVALIA_DATABASE_SIMPLE_SCHEDULER_H__
#define __CAVALIA_DATABASE_SIMPLE_SCHEDULER_H__

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
		class SimpleScheduler : public BaseScheduler {
		public:
			SimpleScheduler(IORedirector *const redirector, ConcurrentExecutor* executor, const size_t &thread_count) : BaseScheduler(redirector, thread_count), executor_(executor), last_allotted_batch_idx_(thread_count), batches_(thread_count) {}
			virtual ~SimpleScheduler(){}
			virtual void Initialize(const size_t& thread_id);
            virtual ParamBatch* GetNextBatch(const size_t& thread_id);
		private:
			SimpleScheduler(const SimpleScheduler&);
			SimpleScheduler& operator=(const SimpleScheduler&);
		protected:
			std::vector<int> last_allotted_batch_idx_;
            std::vector<std::vector<ParamBatch*>*> batches_;
            ConcurrentExecutor* executor_;
		};
	}
}

#endif