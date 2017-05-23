#pragma once
#ifndef __CAVALIA_DATABASE_BASE_SCHEDULER_H__
#define __CAVALIA_DATABASE_BASE_SCHEDULER_H__

#include <TimeMeasurer.h>
#include <AllocatorHelper.h>
#include "../Redirector/IORedirector.h"
#include "../Transaction/StoredProcedure.h"
#include "../Storage/BaseStorageManager.h"
#include "../Logger/BaseLogger.h"

namespace Cavalia{
	namespace Database{
		class BaseScheduler{
		public:
			BaseScheduler(IORedirector *const redirector, const size_t &thread_count) : redirector_ptr_(redirector), thread_count_(thread_count){}
			virtual ~BaseScheduler(){}
			virtual void Initialize(const size_t& thread_id) = 0;
            virtual ParamBatch* GetNextBatch(const size_t& thread_id) = 0;
		private:
			BaseScheduler(const BaseScheduler &);
			BaseScheduler& operator=(const BaseScheduler &);
		protected:
			IORedirector *const redirector_ptr_;
			size_t thread_count_;
		};
	}
}

#endif