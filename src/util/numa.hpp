#ifndef LINEAIRDB_UTIL_NUMA_HPP
#define LINEAIRDB_UTIL_NUMA_HPP

#ifndef __APPLE__
#include <numa.h>
#include <unistd.h>
#endif

namespace LineairDB {
namespace Util {
namespace NUMA {

/**
 * @brief Set the affinity of cpu and thread
 * @param id
 * @note we
 */
void SetAffinity(const size_t id) {
#ifndef __APPLE__
  const auto pid     = getpid();
  auto* mask         = numa_bitmask_alloc();
  const auto cpu_bit = id % mask->size;
  numa_bitmask_setbit(mask, cpu_bit);

  numa_sched_setaffinity(pid, mask);
  numa_free_cpumask();
#endif
}

}  // namespace NUMA

}  // namespace Util

}  // namespace LineairDB

#endif /* LINEAIRDB_UTIL_NUMA_HPP */
