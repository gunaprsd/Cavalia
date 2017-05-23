#include "SimpleScheduler.h"
#include "../Executor/ConcurrentExecutor.h"

using namespace Cavalia;
using namespace Cavalia::Database;

void SimpleScheduler::Initialize(const size_t& thread_id) {
    std::vector<ParamBatch*>* execution_batches = new std::vector<ParamBatch*>();
    std::vector<ParamBatch*>* input_batches = redirector_ptr_->GetParameterBatches(thread_id);
    for (size_t i = 0; i < input_batches->size(); ++i) {
        ParamBatch *tuple_batch = input_batches->at(i);
        // copy to local memory.
        ParamBatch *execution_batch = new ParamBatch(gParamBatchSize);
        for (size_t j = 0; j < tuple_batch->size(); ++j) {
            TxnParam *entry = tuple_batch->get(j);
            // copy each parameter.
            CharArray str;
            entry->Serialize(str);
            TxnParam* new_tuple = executor_->DeserializeParam(entry->type_, str);
            execution_batch->push_back(new_tuple);
            str.Clear();
            delete entry;
            entry = NULL;
        }
        (*execution_batches).push_back(execution_batch);
        delete tuple_batch;
        tuple_batch = NULL;
    }

    last_allotted_batch_idx_[thread_id] = -1;
    batches_[thread_id] = execution_batches;
}

ParamBatch* SimpleScheduler::GetNextBatch(const size_t& thread_id) {
    int next_idx = ++last_allotted_batch_idx_[thread_id];
    if(next_idx < batches_[thread_id]->size()) {
        return (*batches_[thread_id])[next_idx];
    } else {
        return NULL;
    }
}