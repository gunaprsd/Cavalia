#if defined(LOCK)
#include <iostream>
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			if (tb_record->content_.TryWriteLock() == false){
				this->AbortTransaction();
				return false;
			}
			tb_record->record_->is_visible_ = true;
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			access->timestamp_ = 0;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			s_record = t_record->record_;
			switch (access_type) {
				case READ_ONLY:
				{
					if (t_record->content_.TryReadLock() == false) {
						this->AbortTransaction();
						return false;
					}
				}
				//continues execution
				case NO_CC_READ_ONLY:
				{
					Access *access = access_list_.NewAccess();
					access->access_type_ = access_type;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
				case READ_WRITE:
				{
					if (t_record->content_.TryWriteLock() == false) {
						this->AbortTransaction();
						return false;
					}
				}
				//continues execution
				case NO_CC_READ_WRITE:
				{
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					t_record->record_->CopyTo(local_record);
					Access *access = access_list_.NewAccess();
					access->access_type_ = access_type;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
				case DELETE_ONLY:
				{
					if (t_record->content_.TryWriteLock() == false){
						this->AbortTransaction();
						return false;
					}
				}
				//continues execution
				case NO_CC_DELETE_ONLY:
				{
					t_record->record_->is_visible_ = false;
					Access *access = access_list_.NewAccess();
					access->access_type_ = access_type;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
				default:
				{
					assert(false);
					return true;
				}
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str) {
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			#if defined(SCALABLE_TIMESTAMP)
				uint64_t max_rw_ts = 0;
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->timestamp_ > max_rw_ts){
						max_rw_ts = access_ptr->timestamp_;
					}
				}
			#endif

			BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			uint64_t curr_epoch = Epoch::GetEpoch();
			#if defined(SCALABLE_TIMESTAMP)
				uint64_t commit_ts = GenerateScalableTimestamp(curr_epoch, max_rw_ts);
			#else
				uint64_t commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp::GetMonotoneTimestamp());
			#endif
			END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->content_;
				switch(access_ptr->access_type_) {
					case READ_WRITE:
					case NO_CC_READ_WRITE:
					case INSERT_ONLY:
					case NO_CC_INSERT_ONLY:
					case DELETE_ONLY:
					case NO_CC_DELETE_ONLY:
						assert(commit_ts >= access_ptr->timestamp_);
						content_ref.SetTimestamp(commit_ts);
						break;
					default:
						assert(false);
				}
			}
			// commit.
			#if defined(VALUE_LOGGING)
				logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
			#elif defined(COMMAND_LOGGING)
				if (context->is_adhoc_ == true){
					logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
				}
				logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, context->txn_type_, param);
			#endif

			// release locks and free memory
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				switch(access_ptr->access_type_) {
					case READ_ONLY:
						access_ptr->access_record_->content_.ReleaseReadLock();
						break;
					case INSERT_ONLY:
					case DELETE_ONLY:
						access_ptr->access_record_->content_.ReleaseWriteLock();
						break;
					case READ_WRITE:
					{
						access_ptr->access_record_->content_.ReleaseWriteLock();
						//clear memory of local copy of record
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						break;
					}
					default:
						break;
				}
			}

			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			// recover updated data and release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
				SchemaRecord *local_record_ptr = access_ptr->local_record_;
				auto &content_ref = access_ptr->access_record_->content_;
				switch(access_ptr->access_type_) {
					case READ_ONLY:
						content_ref.ReleaseReadLock();
						break;
					case INSERT_ONLY:
						global_record_ptr->is_visible_ = false;
						content_ref.ReleaseWriteLock();
						break;
					case DELETE_ONLY:
						global_record_ptr->is_visible_ = true;
						content_ref.ReleaseWriteLock();
						break;
					case READ_WRITE:
						global_record_ptr->CopyFrom(local_record_ptr);
						content_ref.ReleaseWriteLock();
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						break;
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}

#endif
