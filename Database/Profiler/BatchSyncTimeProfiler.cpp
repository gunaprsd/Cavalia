#include "BatchSyncTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long* access_batch_sync_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *access_batch_sync_timer_;
#else
		TimeMeasurer *access_batch_sync_timer_;
#endif
	}
}
