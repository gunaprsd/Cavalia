#include "SharedWorklistScheduler.h"
#include "../Executor/ConcurrentExecutor.h"

using namespace Cavalia;
using namespace Cavalia::Database;


void SharedWorklistScheduler::Initialize(const size_t& thread_id) {
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


void SharedWorklistScheduler::SynchronizeBatchExecution(const size_t& thread_id) {
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

void SharedWorklistScheduler::ThreadRun() {
    BEGIN_PARTITIONING_TIME_MEASURE(0);
    size_t num_batches = raw_batches_[0]->size();
    for(int batch_idx = 0; batch_idx < num_batches; batch_idx++) {
        batches_.push_back(DoDataBasedPartition(batch_idx));
    }
    END_PARTITIONING_TIME_MEASURE(0);

    std::cout << "done with scheduler initial run..." << std::endl;
    executor_->is_scheduler_ready_ = true;
}

SimpleConcurrentWorklist* SharedWorklistScheduler::DoSimplePartition(int batch_idx) {
    //each batch_idx corresponds to a concurrent-worklist
    SimpleConcurrentWorklist* wl = new SimpleConcurrentWorklist();
    for(int i = 0; i < thread_count_; i++) {
        //each thread has a part of the super-batch
        ParamBatch* thread_param_batch = (*raw_batches_[i])[batch_idx];
        if(thread_param_batch != NULL) {
            int size = thread_param_batch->size();
            for(int j = 0; j < size; ) {
                ParamBatch* atomic_batch = new ParamBatch(MAX_ATOMIC_BATCH_SIZE);
                for(int k = 0; k < MAX_ATOMIC_BATCH_SIZE && j < size; k++) {
                    atomic_batch->push_back(thread_param_batch->get(j));
                    j++;
                }
                wl->Add(atomic_batch);
            }
            delete thread_param_batch;
        }
        
    }
    return wl;
}

SimpleConcurrentWorklist* SharedWorklistScheduler::DoDataBasedPartition(int batch_idx) {
    std::unordered_set<TxnParam*> clusters;
    ReadWriteSet batch_rw_set;
    
    /* Step 1: Create singleton clusters and build read-write sets of txn */
    for(int i = 0; i < thread_count_; i++) {
        ParamBatch* thread_param_batch = (*raw_batches_[i])[batch_idx];
        if(thread_param_batch != NULL) {
            int size = thread_param_batch->size();
            for(int j = 0; j < size; j++) {
                TxnParam* txn = thread_param_batch->get(j);
                txn->BuildReadWriteSet(batch_rw_set);
                clusters.insert(txn);
            }
            delete thread_param_batch;
        }
    }

    size_t num_txns = clusters.size();
    
    std::unordered_set<BatchAccessInfo*> interesting_items;
    size_t max_progress = 0;
    /* Step 2: Collect all interesting data items. Interesting data
    items are those that are accessed by more than one transaction */
    for(auto iter = batch_rw_set.lookup_.begin(); iter != batch_rw_set.lookup_.end(); iter++) {
        BatchAccessInfo* info = iter->second;
        size_t size = info->txns_.size();
        if(size > 1 && size < MAX_ATOMIC_BATCH_SIZE) {
            interesting_items.insert(info);
            max_progress = std::max(max_progress, info->txns_.size());
        } else if(size == 1) {
            info->avoid_cc_ = true;
        }
    }

    /* Step 3: Iteratively cluster txns grouped by data items from the interesting_items set. 
    Progress indicator ensures that the loop terminates. We look at every data item
    only once and merge them if the size constraints are satisfied */
    size_t progress = 2;
    std::vector<BatchAccessInfo*> targets;
    while(progress <= max_progress) {
        //collect target items for this round
        for(auto iter = interesting_items.begin(); iter != interesting_items.end(); iter++) {
            BatchAccessInfo* info = (*iter);
            if(info->txns_.size() <= progress) {
                targets.push_back(*iter);
            }
        }

        //for each target item, try to merge them into a cluster
        for(auto iter = targets.begin(); iter != targets.end(); iter++) {
            BatchAccessInfo* info = *iter;
            
            //find resulting cluster size, if we merge clusters of this data item
            size_t resulting_cluster_size = 0;
            for(auto txn_iter = info->txns_.begin(); txn_iter != info->txns_.end(); txn_iter++) {
                TxnParam* txn = *txn_iter;
                if(txn->data_ != NULL) {
                    resulting_cluster_size += ((ClusterInfo*)txn->data_)->GetSize();
                } else {
                    resulting_cluster_size++;
                }
            }

            if(resulting_cluster_size < MAX_ATOMIC_BATCH_SIZE) {
                while(info->txns_.size() > 1) {
                    auto iter = info->txns_.begin();
                    auto t1 = *iter; 
                    iter++;
                    auto t2 = *iter;
                    auto merged = ClusterInfo::Merge(t1, t2);
                    if(merged == t1) {
                        clusters.erase(t2);
                    } else {
                        clusters.erase(t1);
                    }
                }
                info->avoid_cc_ = true;
                interesting_items.erase(info);
            } else {
                //remove from interesting items so that we don't get it again
                interesting_items.erase(info);
            }
        }
        progress++;
    }
    

    /* Step 4: now we use the produced clusters to partition the super-batch into 
    atomic-batches of size atmost MAX_ATOMIC_BATCH_SIZE. Here we greedily fill the
    buckets. We can do slightly better by sorting and distributing appropriately*/

    //int num_atomic_batches = (num_txns / MAX_ATOMIC_BATCH_SIZE) + 1;
    SimpleConcurrentWorklist* wl = new SimpleConcurrentWorklist();
    
    ParamBatch* current_batch = new ParamBatch(MAX_ATOMIC_BATCH_SIZE);
    size_t current_batch_size = 0;
    for(auto iter = clusters.begin(); iter != clusters.end(); iter++) {
        TxnParam* param = *iter;
        size_t size_of_cluster = param->data_ == NULL ? 1 : ((ClusterInfo*)param->data_)->GetSize();
        bool can_add_in_same_batch = (current_batch_size + size_of_cluster) < MAX_ATOMIC_BATCH_SIZE;

        if(!can_add_in_same_batch) {
            wl->Add(current_batch);
            current_batch = new ParamBatch(MAX_ATOMIC_BATCH_SIZE);
            current_batch_size = 0;
        }
        
        if(param->data_ == NULL) {
            current_batch->push_back(param);
            current_batch_size++;
        } else {
            ClusterInfo* info = (ClusterInfo*)param->data_;
            for(auto txn_iter = info->members_.begin(); txn_iter != info->members_.end(); txn_iter++) {
                current_batch->push_back(*iter);
                current_batch_size++;
            }
            delete info;
        }
    }
    //adding last batch
    wl->Add(current_batch);
    return wl;
}

ParamBatch* SharedWorklistScheduler::GetNextBatch(const size_t& thread_id) {
    int current_batch_idx = current_batch_idx_.load();
    if(current_batch_idx < batches_.size()) {
        //current super-batch is active : try getting from current super-batch
        ParamBatch* batch = batches_[current_batch_idx]->GetNext();
        if(batch != NULL) {
            return batch;
        } else {
            SynchronizeBatchExecution(thread_id);
            return GetNextBatch(thread_id);
        }
    } else {
        return NULL;
    }
}

bool compare_degree(BatchAccessInfo* info1, BatchAccessInfo* info2) {
    return info1->txns_.size() < info2->txns_.size();
}