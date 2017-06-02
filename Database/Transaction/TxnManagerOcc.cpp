#if defined(OCC) || defined(DYNAMIC_CC)
#include "TransactionManager.h"

namespace Cavalia {
	namespace Database {
		/*
		** Optimistic Concurrency Control: 
		** -------------------------------
		** Enabled by -DOCC compiler flag
		**
		** Implementation of a vanilla OCC. SelectCC command (READ_ONLY, READ_WRITE or DELETE_ONLY) reads data from 
		** table as is and records the timestamp. For READ_WRITE access, in addition it creates a local copy of the data.
		** During the validation phase, the accesses are sorted based on timestamp (ascending). For each access, appropriate
		** lock (READ_LOCK or WRITE_LOCK) is acquired and then validated by comparing the timestamps. If validation does not 
		** succeed, all acquired locks are released and the transaction is aborted. If validation succeeds, all changes 
		** are reflected on the global table and logged. Then locks are released and data structures cleaned up.
		*/

		
		#if defined(DYNAMIC_CC)
			CC_INSERT_FUNCTION_HEADER_SPECIFIED(Occ)
		#else 
			bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) 
		#endif
		{
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			// insert with visibility bit set to false.
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
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
			CC_SELECT_FUNCTION_HEADER_SPECIFIED(Occ)
		#else
			bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) 
		#endif
		{
			switch(access_type) {
				case READ_ONLY: 
				{
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					s_record = t_record->record_;
					return true;
				}
				case READ_WRITE: 
				{
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					// copy data
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					access->timestamp_ = t_record->content_.GetTimestamp();
					COMPILER_MEMORY_FENCE;
					local_record->CopyFrom(t_record->record_);
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					// reset returned record.
					s_record = local_record;
					return true;
				}
				// we just need to set the visibility bit on the record. so no need to create local copy.
				case DELETE_ONLY: 
				{
					Access *access = access_list_.NewAccess();
					access->access_type_ = DELETE_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					s_record = t_record->record_;
					return true;
				}
				#if defined(SELECTIVE_CC) 
					case NO_CC_READ_ONLY: 
					{
						Access *access = access_list_.NewAccess();
						access->access_type_ = READ_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->content_.GetTimestamp();
						s_record = t_record->record_;
						return true;
					}
					case NO_CC_READ_WRITE: 
					{
						//no need to have a compiler fence
						Access *access = access_list_.NewAccess();
						access->access_type_ = READ_WRITE;
						access->access_record_ = t_record;
						// copy data
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
						char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
						SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(local_record)SchemaRecord(schema_ptr, local_data);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						local_record->CopyFrom(t_record->record_);
						COMPILER_MEMORY_FENCE
						access->timestamp_ = t_record->content_.GetTimestamp();
						access->local_record_ = local_record;
						access->table_id_ = table_id;
						// reset returned record.
						s_record = local_record;
						return true;
					}
					// we just need to set the visibility bit on the record. so no need to create local copy.
					case NO_CC_DELETE_ONLY: 
					{
						Access *access = access_list_.NewAccess();
						access->access_type_ = DELETE_ONLY;
						access->access_record_ = t_record;
						access->local_record_ = NULL;
						access->table_id_ = table_id;
						access->timestamp_ = t_record->content_.GetTimestamp();
						s_record = t_record->record_;
						return true;
					}
				#endif
				default: 
				{
					assert(false);
					return true;
				}
			}
		}

		#if defined(DYNAMIC_CC)
			CC_COMMIT_FUNCTION_HEADER_SPECIFIED(Occ)
		#else
			bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str) 
		#endif
		{
			bool is_success = true;
			size_t lock_count = 0;
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// Step 1: Acquire locks and validate
			VALIDATION_PHASE:
			{
				access_list_.Sort();
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					++lock_count;
					Access *access_ptr = access_list_.GetAccess(i);
					auto &content_ref = access_ptr->access_record_->content_;
					switch(access_ptr->access_type_) {
						case READ_ONLY: 
						{
							content_ref.AcquireReadLock();
							if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
								UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
								is_success = false;
								goto RELEASE_LOCKS_AND_CLEANUP;
							}
							break;
						}
						case READ_WRITE: 
						{
							content_ref.AcquireWriteLock();
							if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
								UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
								is_success = false;
								goto RELEASE_LOCKS_AND_CLEANUP;;
							}
							break;
						}
						case INSERT_ONLY:
						case DELETE_ONLY: 
						{
							content_ref.AcquireWriteLock();
							break;
						}
						#if defined(SELECTIVE_CC)
							case NO_CC_READ_ONLY:
							case NO_CC_READ_WRITE:
							case NO_CC_INSERT_ONLY:
							case NO_CC_DELETE_ONLY:
								break;
						#endif
						default:
						{
							assert(false);
						}
					}
				}
			}
			
			// Step 2: If success, then overwrite and commit
			COMMIT:
			{
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

				//updating the table
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					auto &content_ref = access_ptr->access_record_->content_;
					switch(access_ptr->access_type_) {
						case READ_WRITE: 
						{
							assert(commit_ts > access_ptr->timestamp_);
							global_record_ptr->CopyFrom(local_record_ptr);
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
							break;
						}
						case INSERT_ONLY: 
						{
							assert(commit_ts > access_ptr->timestamp_);
							global_record_ptr->is_visible_ = true;
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
							break;
						}
						case DELETE_ONLY: 
						{
							assert(commit_ts > access_ptr->timestamp_);
							global_record_ptr->is_visible_ = false;
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
							break;
						}
						#if defined(SELECTIVE_CC)
							case NO_CC_READ_WRITE: 
							{
								assert(commit_ts > access_ptr->timestamp_);
								global_record_ptr->CopyFrom(local_record_ptr);
								COMPILER_MEMORY_FENCE
								content_ref.SetTimestamp(commit_ts);
								break;
							}
							case NO_CC_INSERT_ONLY: 
							{
								assert(commit_ts > access_ptr->timestamp_);
								global_record_ptr->is_visible_ = true;
								COMPILER_MEMORY_FENCE
								content_ref.SetTimestamp(commit_ts);
								break;
							}
							case NO_CC_DELETE_ONLY: 
							{
								assert(commit_ts > access_ptr->timestamp_);
								global_record_ptr->is_visible_ = false;
								COMPILER_MEMORY_FENCE
								content_ref.SetTimestamp(commit_ts);
								break;
							}
						#endif
						default:
							break;
					}
				}
				
				#if defined(VALUE_LOGGING)
					logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
				#elif defined(COMMAND_LOGGING)
					if (context->is_adhoc_ == true){
						logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
					}
					logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, context->txn_type_, param);
				#endif
			}
			
			//Step 3: Release locks and free memory
			RELEASE_LOCKS_AND_CLEANUP:
			{
				for (size_t i = 0; i < lock_count; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					switch(access_ptr->access_type_) {
						case READ_ONLY: 
						{
							access_ptr->access_record_->content_.ReleaseReadLock();
							break;
						}
						case READ_WRITE: 
						{
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							SchemaRecord *local_record_ptr = access_ptr->local_record_;
							MemAllocator::Free(local_record_ptr->data_ptr_);
							local_record_ptr->~SchemaRecord();
							MemAllocator::Free((char*)local_record_ptr);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							break;
						}
						case INSERT_ONLY:
						case DELETE_ONLY: 
						{
							access_ptr->access_record_->content_.ReleaseWriteLock();
							break;
						}
						#if defined(SELECTIVE_CC)
							case NO_CC_READ_WRITE: 
							{
								BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
								SchemaRecord *local_record_ptr = access_ptr->local_record_;
								MemAllocator::Free(local_record_ptr->data_ptr_);
								local_record_ptr->~SchemaRecord();
								MemAllocator::Free((char*)local_record_ptr);
								END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
								break;
							}
							case NO_CC_INSERT_ONLY:
							case NO_CC_DELETE_ONLY: 
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
			
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		#if defined(DYNAMIC_CC)
			CC_ABORT_FUNCTION_HEADER_SPECIFIED(Occ)
		#else
			void TransactionManager::AbortTransaction(TxnContext* context) 
		#endif
		{
			assert(false);
		}
	}
}

#endif
