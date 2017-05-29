#include "PartitioningTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long* access_partitioning_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *access_partitioning_timer_;
#else
		TimeMeasurer *access_partitioning_timer_;
#endif
	}
}
