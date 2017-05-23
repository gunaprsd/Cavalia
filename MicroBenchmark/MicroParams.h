#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_PARAMS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_PARAMS_H__

#include <Transaction/TxnParam.h>
#include <vector>
#include "MicroRecords.h"
#include "MicroMeta.h"
#include "MicroConstants.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			using namespace Cavalia::Database;
			class MicroParam : public TxnParam{
			public:
				MicroParam(){
					type_ = MICRO;
				}
				virtual ~MicroParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t) * NUM_ACCESSES);
					serial_str.Memcpy(0, reinterpret_cast<const char*>(keys_), sizeof(int64_t) * NUM_ACCESSES);
				}

				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) * NUM_ACCESSES;
					memcpy(buffer, reinterpret_cast<const char*>(keys_), sizeof(int64_t) * NUM_ACCESSES);
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(keys_), serial_str.char_ptr_, sizeof(int64_t) * NUM_ACCESSES);
				}

				virtual std::vector<AccessWrapper>* GetReadWriteSet() {
					if(rw_set_ == NULL) {
						rw_set_ = new std::vector<AccessWrapper>(NUM_ACCESSES);
						for (size_t i = 0; i < NUM_ACCESSES / 2; ++i) {
							(*rw_set_)[i].hash_code_ = keys_[i];
							(*rw_set_)[i].access_type_ = READ_ONLY;
						}
						for (size_t i = NUM_ACCESSES / 2; i < NUM_ACCESSES; ++i) {
							(*rw_set_)[i].hash_code_ = keys_[i];
							(*rw_set_)[i].access_type_ = READ_WRITE;
						}
					}
					return rw_set_;
				}

			public:
				int64_t keys_[NUM_ACCESSES];
			};
		}
	}
}

#endif
