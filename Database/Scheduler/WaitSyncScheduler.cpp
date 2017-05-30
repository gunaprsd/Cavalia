#include "WaitSyncScheduler.h"
#include "../Executor/ConcurrentExecutor.h"

using namespace Cavalia;
using namespace Cavalia::Database;


void WaitSyncScheduler::Initialize(const size_t& thread_id) {
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
    raw_batches_[thread_id] = execution_batches;
}


void WaitSyncScheduler::SynchronizeBatchExecution(const size_t& thread_id) {
    BEGIN_BATCH_SYNC_TIME_MEASURE(thread_id);
    int next_batch_idx = current_batch_idx_.load() + 1;
    lock_.lock();
    waiting_threads_.push_back(thread_id);
    if(waiting_threads_.size() == thread_count_) {
        waiting_threads_.clear();
        current_batch_idx_.fetch_add(1);
    }
    lock_.unlock();
    while(current_batch_idx_.load() < next_batch_idx);
    END_BATCH_SYNC_TIME_MEASURE(thread_id);
}

void WaitSyncScheduler::ThreadRun() {
    
    for(int i = 0; i < thread_count_; i++) {
        batches_[i] = new std::vector<ParamBatch*>();
    }

    size_t num_batches = raw_batches_[0]->size();
    BEGIN_PARTITIONING_TIME_MEASURE(0); 
    for(int batch_idx = 0; batch_idx < num_batches; batch_idx++) {
        for(int i = 0; i < thread_count_; i++) {
            ParamBatch* param_batch = (*raw_batches_[i])[batch_idx];
            if(param_batch != NULL) {
                batches_[i]->push_back(param_batch);
            }
        }
    }
    END_PARTITIONING_TIME_MEASURE(0);

    std::cout << "done with scheduler initial run..." << std::endl;
    executor_->is_scheduler_ready_ = true;
}

ParamBatch* WaitSyncScheduler::GetNextBatch(const size_t& thread_id) {
    int next_batch_idx = current_batch_idx_.load() + 1;
    if(next_batch_idx < batches_[thread_id]->size()) {
        SynchronizeBatchExecution(thread_id);
        int batch_idx = current_batch_idx_.load();
        return (*batches_[thread_id])[batch_idx];
    } else {
        return NULL;
    }
}