#pragma once
#ifndef __CAVALIA_DATABASE_LOG_ENTRY_H__
#define __CAVALIA_DATABASE_LOG_ENTRY_H__

#include <cstdint>
#include "../Transaction/TxnParam.h"

namespace Cavalia{
	namespace Database{

		struct BaseLogEntry{
			BaseLogEntry(const bool is_command_log) : timestamp_(0), is_command_log_(is_command_log){}
			BaseLogEntry(const uint64_t &timestamp, const bool is_command_log) : timestamp_(timestamp), is_command_log_(is_command_log){}
			uint64_t timestamp_;
			bool is_command_log_;
		};

		typedef std::vector<BaseLogEntry*> BaseLogEntries;

		struct CommandLogEntry : public BaseLogEntry{
			CommandLogEntry() : BaseLogEntry(true){}
			CommandLogEntry(const uint64_t &timestamp, TxnParam *param) : BaseLogEntry(timestamp, true), param_(param){}

			TxnParam *param_;
		};

		typedef std::vector<CommandLogEntry*> CommandLogEntries;

		struct ValueLogElement{
			uint8_t type_;
			uint8_t table_id_;
			uint8_t data_size_;
			char *data_ptr_;
		};

		struct ValueLogEntry : public BaseLogEntry{
			ValueLogEntry() : BaseLogEntry(false), element_count_(0){}

			ValueLogElement* NewValueLogElement(){
				assert(element_count_ < kMaxAccessNum);
				ValueLogElement *ret = &elements_[element_count_];
				++element_count_;
				return ret;
			}

			void Clear(){
				element_count_ = 0;
			}

			ValueLogElement elements_[kMaxAccessNum];
			size_t element_count_;
		};

		typedef std::vector<ValueLogEntry*> ValueLogEntries;
	}
}

#endif