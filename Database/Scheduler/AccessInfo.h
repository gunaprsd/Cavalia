#pragma once
#ifndef __CAVALIA_DATABASE_ACCESS_INFO_H__
#define __CAVALIA_DATABASE_ACCESS_INFO_H__

#include <vector>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include "../Transaction/TxnParam.h"
namespace Cavalia{
	namespace Database{
        class ConcurrentExecutor;

		//This object contains information about each data item in current batch
		struct BatchAccessInfo
		{
			int64_t hash_; //data hash
			bool has_writes_; //for optimizations
			bool avoid_cc_;
			std::unordered_set<TxnParam*> txns_; //txn clusters in current batch that access data
			BatchAccessInfo(int64_t hash) : txns_(), hash_(hash), has_writes_(false), avoid_cc_(false) {}
			inline void AddTransaction(TxnParam* txn) { txns_.insert(txn); }
			inline void RemoveTransaction(TxnParam* txn) { txns_.erase(txn); }
		};

		struct ReadWriteSet 
		{
			std::unordered_map<int64_t, BatchAccessInfo*> lookup_;
			ReadWriteSet() : lookup_() {}
			inline bool CanAvoidConcurrencyControl(int64_t hash) { return lookup_[hash] == NULL? false : lookup_[hash]->avoid_cc_; }	
			inline BatchAccessInfo* Get(int64_t hash) { return lookup_[hash]; }
			inline void Add(int64_t hash, BatchAccessInfo* info) { lookup_[hash] = info; }
			inline BatchAccessInfo* GetOrAdd(int64_t hash) {
				auto info = lookup_[hash];
				if(info == NULL) {
					info = new BatchAccessInfo(hash);
					lookup_[hash] = info;
				}
				return info;
			}
		};

		
        //ClusterInfo is the data structure that is present in a representative
		//TxnParam. The invariant is that this TxnParam is listed in nbrs of 
		//all the data items accessed by the txns in the cluster.
		struct ClusterInfo
		{
			TxnParam* owner_;
			std::vector<TxnParam*> members_;
			std::unordered_set<BatchAccessInfo*> items_accessed_;
			
			ClusterInfo(TxnParam* owner) : owner_(owner), members_(), items_accessed_() {
				AddMembers(owner_);
				AddNeighborsOf(owner_);
			}

			inline void AddNeighborsOf(TxnParam* txn) {
				//TxnParam could either be a cluster or a single transaction
				if(txn->data_ == NULL) {
					//single transaction
					auto rw_set = txn->GetReadWriteSet();
					auto iter = rw_set.lookup_.begin();
					for(; iter != rw_set.lookup_.end(); iter++) {
						BatchAccessInfo* info = iter->second;
						items_accessed_.insert(info);
					}
				} 
				else {
					//it is a cluster: add all from the cached_list
					ClusterInfo* info = (ClusterInfo*)txn->data_;
					items_accessed_.insert(info->items_accessed_.begin(), info->items_accessed_.end());
				}
			}

			//In all BatchAccessInfo that are neighbors to other, 
			//remove other and add this.
			inline void ReplaceInNeighbors(TxnParam* other) {
				if(other->data_ == NULL) {
					//single transaction
					auto rw_set = other->GetReadWriteSet();
					auto iter = rw_set.lookup_.begin();
					for(; iter != rw_set.lookup_.end(); iter++) {
						BatchAccessInfo* info = iter->second;
						info->RemoveTransaction(other);
						info->AddTransaction(owner_); //set semantics, don't worry!
					}
				} 
				else {
					//it is a cluster: add all from the cached_list
					ClusterInfo* info = (ClusterInfo*)other->data_;
					auto iter = info->items_accessed_.begin();
					for(; iter != info->items_accessed_.end(); iter++) {
						BatchAccessInfo* binfo = *iter;
						binfo->RemoveTransaction(other);
						binfo->AddTransaction(owner_); //set semantics, don't worry!
					}
				}
			}

			inline void AddMembers(TxnParam* other) {
				if(other->data_ == NULL) {
					members_.push_back(other);
				} else {
					ClusterInfo* info = (ClusterInfo*)other->data_;
					for(auto iter = info->members_.begin(); iter != info->members_.end(); iter++) {
						members_.push_back(*iter);
					}
				}
			}
			
			inline void MergeClusters(TxnParam* other) {
				//In merging clusters, there are two actions.
				//Replace 'other' occurrences with this in all data items
				//Add data items in 'other' as neighbors to this.
				ReplaceInNeighbors(other);
				AddNeighborsOf(other);
				AddMembers(other); //add the members from the cluster
			}

			inline size_t GetSize() {
				return members_.size();
			}

			//Merges t2 to t1 or t1 to t2 and returns the representative 
			//transaction that has the ClusterInfo
			static TxnParam* Merge(TxnParam* t1, TxnParam* t2) {
				bool t1_is_cluster = t1->data_ != NULL;
				bool t2_is_cluster = t2->data_ != NULL;

				if(t1_is_cluster && t2_is_cluster) {
					//choose the smaller one to merge with the larger one
					ClusterInfo *info1 = (ClusterInfo*)t1->data_, *info2 = (ClusterInfo*)t2->data_;
					if(info1->GetSize() < info2->GetSize()) {
						//merge t1 with t2
						info2->MergeClusters(t1);
						delete info1;
						t1->data_ = NULL;
						return t2;
					} else {
						//merge t2 with t1
						info1->MergeClusters(t2);
						delete info2;
						t2->data_ = NULL;
						return t1;
					}
				} else if(t1_is_cluster && !t2_is_cluster) {
					//merge t2 with t1
					ClusterInfo* info1 = (ClusterInfo*)t1->data_;
					info1->MergeClusters(t2);
					return t1;
				} else if(!t1_is_cluster && t2_is_cluster) {
					//merge t1 with t2
					ClusterInfo* info2 = (ClusterInfo*)t2->data_;
					info2->MergeClusters(t1);
					return t2;
				} else {
					//create a new clusterinfo
					ClusterInfo* info1 = CreateClusterInfo(t1);
					info1->MergeClusters(t2);
					return t1;
				}
			}

			static ClusterInfo* CreateClusterInfo(TxnParam* param) {
				assert(param->data_ == NULL);
				ClusterInfo* info1 = new ClusterInfo(param);
				param->data_ = (char*)info1;
				return info1;
			}
		};
    }
}

#endif