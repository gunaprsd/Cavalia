#include "MicroSource.h"
namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			void MicroSource::StartGeneration() {
#if defined(DEBUG_DIST)
				int* count = new int[num_items_ + 1];
				memset(count, 0, sizeof(int)* (num_items_ + 1));
#endif
				int numBatches = 0;
				ParamBatch *tuples = new ParamBatch();
				for (size_t i = 0; i < num_transactions_; ++i) {
					//Generating parameters for a single txn
					//We do not let an item repeat in the same txn
					MicroParam* param = new MicroParam();
					std::unordered_set<int> keys;
					for (size_t access_id = 0; access_id < NUM_ACCESSES; ++access_id){
						int res = random_generator_.GenerateZipfNumber();
						while (keys.find(res) != keys.end()){
							res = random_generator_.GenerateZipfNumber();
						}
						keys.insert(res);
						param->keys_[access_id] = static_cast<int64_t>(res);
#if defined(DEBUG_DIST)
						count[res]++;
#endif
					}
					tuples->push_back(param);
					keys.clear();

					//Dump the txns params of a batch size to disk
					if ((i + 1) % gParamBatchSize == 0){
						DumpToDisk(tuples);
						//Round-robin adding to the different threads
						redirector_ptr_->PushParameterBatch(tuples);
						numBatches++;
						tuples = new ParamBatch();
					}
				}

				//If any left do the honors else free memory 
				if (tuples->size() != 0){
					DumpToDisk(tuples);
					redirector_ptr_->PushParameterBatch(tuples);
				} else{
					delete tuples;
					tuples = NULL;
				}

				printf("Added %d batches each of size %ld to the redirector\n", numBatches, gParamBatchSize);
				//useful for debugging!
#if defined(DEBUG_DIST)
				std::cout << "access distribution from source:" << std::endl;
				for (int i = 1; i <= 100; ++i){
					std::cout << i << "," << count[i] << std::endl;
				}
				delete[] count;
#endif
			}
		}
	}
}
