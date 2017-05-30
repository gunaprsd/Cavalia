#include "AccessInfo.h"

using namespace Cavalia;
using namespace Cavalia::Database;



// void TransactionNodeData::merge(TxnParam* param) {
//     TransactionNodeData* data = (TransactionNodeData*)param->data_;
//     assert(data != NULL);

//     //perform graph operations
//     for(auto iterator = data->cached_items_.begin(); iterator != data->cached_items_.end(); iterator++) {
//         DataItem* item = *iterator;
//         cached_items_.insert(item);
//         item->params_.remove(param);
//         item->params_.insert(owner_);
//     }
    
//     //merge the clusters
//     size_ += data->size_;
//     last_->next_ = param;
//     last_->last_ = data->last_;
// }

// bool TransactionNodeData::can_merge(TxnParam* param) {
//     TransactionNodeData* data = (TransactionNodeData*)param->data_;
//     assert(data != NULL);
//     return (size_ + data->size_) < MAX_CLUSTER_SIZE;
// }
