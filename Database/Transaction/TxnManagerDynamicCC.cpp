#if defined(DYNAMIC_CC)
#include <iostream>
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			switch(context->cc_type_) {
                case CC_LOCK_WAIT:
                    return CC_INSERT_FUNCTION(LockWait)(context, table_id, primary_key, record);
                case CC_OCC:
                    return CC_INSERT_FUNCTION(Occ)(context, table_id, primary_key, record);
                default:
                    assert(false);
                    return true;
            }
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			switch(context->cc_type_) {
                case CC_LOCK_WAIT:
                    return CC_SELECT_FUNCTION(LockWait)(context, table_id, t_record, s_record, access_type);
                case CC_OCC:
                    return CC_SELECT_FUNCTION(Occ)(context, table_id, t_record, s_record, access_type);
                default:
                    assert(false);
                    return true;
            }
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str) {
			switch(context->cc_type_) {
                case CC_LOCK_WAIT:
                    return CC_COMMIT_FUNCTION(LockWait)(context, param, ret_str);
                case CC_OCC:
                    return CC_COMMIT_FUNCTION(Occ)(context, param, ret_str);
                default:
                    assert(false);
                    return true;
            }
		}

		void TransactionManager::AbortTransaction(TxnContext* context) {
			switch(context->cc_type_) {
                case CC_LOCK_WAIT:
                    return CC_ABORT_FUNCTION(LockWait)(context);
                case CC_OCC:
                    return CC_ABORT_FUNCTION(Occ)(context);
                default:
                    assert(false);
            }
		}
	}
}

#endif
