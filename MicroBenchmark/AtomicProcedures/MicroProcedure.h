#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_ATOMIC_PROCEDURES_MICRO_PROCEDURE_H__
#define __CAVALIA_MICRO_BENCHMARK_ATOMIC_PROCEDURES_MICRO_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include <Scheduler/AccessBasedScheduler.h>
#include "../MicroInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			namespace AtomicProcedures{
				class MicroProcedure : public StoredProcedure{
				public:
					MicroProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~MicroProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						MicroParam* micro_param = static_cast<MicroParam*>(param);
						for (size_t i = 0; i < NUM_ACCESSES / 2; ++i){
							SchemaRecord *record = NULL;
							int64_t key = micro_param->keys_[i];
							AccessType type = micro_param->rw_set_.CanAvoidConcurrencyControl(key) ? NO_CC_READ_ONLY : READ_ONLY;
							DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)&key, sizeof(int64_t)), record, type));
							assert(record != NULL);
						}
						for (size_t i = NUM_ACCESSES / 2; i < NUM_ACCESSES; ++i){
							SchemaRecord *record = NULL;
							int64_t key = micro_param->keys_[i];
							AccessType type = micro_param->rw_set_.CanAvoidConcurrencyControl(key) ? NO_CC_READ_WRITE : READ_WRITE;
							DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)&key, sizeof(int64_t)), record, type));
							assert(record != NULL);
						}
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					MicroProcedure(const MicroProcedure&);
					MicroProcedure& operator=(const MicroProcedure&);
				};
			}
		}
	}
}

#endif
