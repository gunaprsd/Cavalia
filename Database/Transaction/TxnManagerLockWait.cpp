#if defined(LOCK_WAIT) || defined(DYNAMIC_CC)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		/*
		** 2 Phase Locking - Wait Die Strategy:
		** ------------------------------------
		** Enabled by compiler flag -DLOCK_WAIT
		** 
		** Implementation of Two phase locking, where locks are released only during the commit time.
		** A request for lock acquisition succeeds when there is no potential deadlock cycle created by waiting. 
		** If request is denied, then transaction is aborted. Lock waiting is implemented using a waiting queue and
		** each thread waits until lock is acquired. It does not wait till the locks are acquired. During commit 
		** phase, all changes are persisted (in-memory and on-disk) and locks released. 
		*/

		#if defined(DYNAMIC_CC)
			CC_INSERT_FUNCTION_HEADER_SPECIFIED(LockWait)
		#else 
			bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) 
		#endif
		{
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true) {
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				#if defined(BATCH_TIMESTAMP)
					if (!batch_ts_.IsAvailable()){
						batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
					}
					start_timestamp_ = batch_ts_.GetTimestamp();
				#else
					start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
				#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
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

		#if defined(DYNAMIC_CC)
			CC_SELECT_FUNCTION_HEADER_SPECIFIED(LockWait)
		#else
			bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) 
		#endif
		{
			s_record = t_record->record_;
			if (is_first_access_ == true) {
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				#if defined(BATCH_TIMESTAMP)
					if (!batch_ts_.IsAvailable()){
						batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
					}
					start_timestamp_ = batch_ts_.GetTimestamp();
				#else
					start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
				#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			volatile bool lock_ready = true;
			switch(access_type) {
				case READ_ONLY: 
				{
					// if cannot get lock, then return immediately.
					if (t_record->wait_content_.AcquireLock(start_timestamp_, LockType::READ_LOCK, &lock_ready) == false) {
						this->AbortTransaction(context);
						return false;
					} else {
						BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
						while (!lock_ready);
						END_CC_WAIT_TIME_MEASURE(thread_id_);
						Access *access = access_list_.NewAccess();
						access->access_type_ = READ_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->wait_content_.GetTimestamp();
						return true;
					}
				}
				case READ_WRITE:
				{
					if (t_record->wait_content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready) == false) {
						this->AbortTransaction(context);
						return false;
					} else {
						BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
						while (!lock_ready);
						END_CC_WAIT_TIME_MEASURE(thread_id_);
						const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
						char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
						SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(local_record)SchemaRecord(schema_ptr, local_data);
						t_record->record_->CopyTo(local_record);
						Access *access = access_list_.NewAccess();
						access->access_type_ = READ_WRITE;
						access->access_record_ = t_record;
						access->local_record_ = local_record;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->wait_content_.GetTimestamp();
						return true;
					}
				}
				case DELETE_ONLY: 
				{
					if (t_record->wait_content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready) == false) {
						this->AbortTransaction(context);
						return false;
					} else {
						BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
						while (!lock_ready);
						END_CC_WAIT_TIME_MEASURE(thread_id_);
						t_record->record_->is_visible_ = false;
						Access *access = access_list_.NewAccess();
						access->access_type_ = DELETE_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->wait_content_.GetTimestamp();
						return true;
					}
				}
				#if defined(SELECTIVE_CC)
					case NO_CC_READ_ONLY: 
					{
						Access *access = access_list_.NewAccess();
						access->access_type_ = NO_CC_READ_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->content_.GetTimestamp();
						return true;
					}
					case NO_CC_READ_WRITE:
					{
						const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
						char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
						SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(local_record)SchemaRecord(schema_ptr, local_data);
						t_record->record_->CopyTo(local_record);
						Access *access = access_list_.NewAccess();
						access->access_type_ = NO_CC_READ_WRITE;
						access->access_record_ = t_record;
						access->local_record_ = local_record;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->content_.GetTimestamp();
						return true;
					}
					case NO_CC_DELETE_ONLY: 
					{
						t_record->record_->is_visible_ = false;
						Access *access = access_list_.NewAccess();
						access->access_type_ = NO_CC_DELETE_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->content_.GetTimestamp();
						return true;
					}
				#endif
				default:
					break;
			}
		}

		#if defined(DYNAMIC_CC)
			CC_COMMIT_FUNCTION_HEADER_SPECIFIED(LockWait)
		#else
			bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str) 
		#endif
		{
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);

			BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			uint64_t curr_epoch = Epoch::GetEpoch();
			#if defined(SCALABLE_TIMESTAMP)
				uint64_t max_rw_ts = 0;
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->timestamp_ > max_rw_ts){
						max_rw_ts = access_ptr->timestamp_;
					}
				}
				uint64_t commit_ts = GenerateScalableTimestamp(curr_epoch, max_rw_ts);
			#else
				uint64_t commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp::GetMonotoneTimestamp());
			#endif
			END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->wait_content_;
				switch(access_ptr->access_type_) {
					case READ_WRITE:
					case INSERT_ONLY:
					case DELETE_ONLY:
						assert(commit_ts >= access_ptr->timestamp_);
						content_ref.SetTimestamp(commit_ts);
						break;
					#if defined(SELECTIVE_CC)
						case NO_CC_READ_WRITE:
						case NO_CC_INSERT_ONLY:
						case NO_CC_DELETE_ONLY:
							assert(commit_ts >= access_ptr->timestamp_);
							content_ref.SetTimestamp(commit_ts);
							break;
					#endif
					default:
						break;
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
			
			// release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				switch(access_ptr->access_type_) {
					case READ_ONLY: 
					{
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						break;
					}
					case READ_WRITE: 
					{
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						break;
					}
					case DELETE_ONLY:
					{
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						break;
					}
					#if defined(SELECTIVE_CC)
						case NO_CC_READ_WRITE:
						{
							MemAllocator::Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							MemAllocator::Free((char*)access_ptr->local_record_);
							break;
						}
						case NO_CC_READ_ONLY:
						case NO_CC_DELETE_ONLY:
							break;
					#endif
					default:
						break;
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		#if defined(DYNAMIC_CC)
			CC_ABORT_FUNCTION_HEADER_SPECIFIED(LockWait)
		#else
			void TransactionManager::AbortTransaction(TxnContext* context) 
		#endif
		{
			// recover updated data and release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				switch(access_ptr->access_type_) {
					case READ_ONLY: 
					{
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						break;
					}
					case READ_WRITE: 
					{
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						access_ptr->access_record_->record_->CopyFrom(access_ptr->local_record_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						break;
					}
					case INSERT_ONLY: 
					{
						access_ptr->access_record_->record_->is_visible_ = false;
						break;
					}
					case DELETE_ONLY: 
					{
						access_ptr->access_record_->record_->is_visible_ = true;
						access_ptr->access_record_->wait_content_.ReleaseLock(start_timestamp_);
						break;
					}
					#if defined(SELECTIVE_CC)
						case NO_CC_READ_WRITE:
						{
							access_ptr->access_record_->record_->CopyFrom(access_ptr->local_record_);
							MemAllocator::Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							MemAllocator::Free((char*)access_ptr->local_record_);
							break;
						}
						case NO_CC_DELETE_ONLY:
						{
							access_ptr->access_record_->record_->is_visible_ = true;
							break;
						}
						case NO_CC_INSERT_ONLY: 
						{
							access_ptr->access_record_->record_->is_visible_ = false;
							break;
						}
						case NO_CC_READ_ONLY:
							break;
					#endif
					default:
						break;
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}

#endif
