#include "../def.hpp"
#include "../containers/relation.hpp"
#include "../containers/bucket_index.hpp"

// sorting condition
bool CompareByStart(const Record& a, const Record& b);

// function to estimate FSs extent
uint64_t FS_estimation(Relation& R, Relation& S);

// single thread execution functions
uint64_t udFS_wc          (Relation &R, Relation &S);
uint64_t udFS_xor         (Relation &R, Relation &S);
uint64_t udFS_simd_xor    (Relation &R, Relation &S);

uint64_t bgudFS_wc          (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_xor         (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_simd_xor    (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);

int get_closest_node(int core_id);
int get_most_distant_node(int core_id);
int stick_this_thread_to_core(int core_id);

uint64_t optFS(Relation& R, Relation&S, int mode, int computations, int max_group_size, int allocationStrategy, int* thread_core_map)
{
	Timer tim;
	BucketIndex BIR, BIS;
	uint64_t result = 0, FS_est = 0;

	// Sort (tbb produces valgrind false memory leaks)
	printf("-------- PRE-JOIN --------\n");
	tim.start();
	oneapi::tbb::task_arena arenaA(1);
	oneapi::tbb::task_arena arenaB(1);
	arenaA.execute([&]{ oneapi::tbb::parallel_sort( &R.record_list[0], &R.record_list[0] + R.numRecords, CompareByStart); });
	arenaB.execute([&]{ oneapi::tbb::parallel_sort( &S.record_list[0], &S.record_list[0] + S.numRecords, CompareByStart); });
//	std::sort( &R.record_list[0], &R.record_list[0] + R.numRecords, CompareByStart);
//	std::sort( &S.record_list[0], &S.record_list[0] + S.numRecords, CompareByStart);
	double timeSorting = tim.stop();
	printf("Sorting:                    %lf secs\n", timeSorting);

	// column store switch
	tim.start();
	R.columnStoreSwitch(thread_core_map[0]);
	S.columnStoreSwitch(thread_core_map[0]);
	double switchingTime = tim.stop();
	printf("Switching to Column Store:  %lf secs\n", switchingTime);

	// selectivity check
	tim.start();
	FS_est = FS_estimation( R, S);
	double selectivityEstimationTime = tim.stop();
	printf("Selectivity Estimation:     %lf secs ", selectivityEstimationTime);

	// buckets (if needed)
	if (FS_est >= OPT_FS_BARRIER)
	{
		printf("* calculated %ld, going for bgudFS *\n", FS_est);
		tim.start();
		BIR.build( R, 1000);
		BIS.build( S, 1000);
		double bucketBuildingTime = tim.stop();
		printf("Bucketing:                  %lf secs\n", bucketBuildingTime);
		printf(" Pre-Join Time:             %lf secs\n", timeSorting + switchingTime + selectivityEstimationTime + bucketBuildingTime);
	}
	else
	{
		printf("* calculated %ld, going for udFS *\n", FS_est);
		printf(" Pre-Join Time:             %lf secs\n", timeSorting + switchingTime + selectivityEstimationTime);
	}
	printf("--------------------------\n");

	// join
	double joinTime;
	printf("\n--------- JOIN ---------\n");
	if (allocationStrategy != OS)
		stick_this_thread_to_core(thread_core_map[0]);
	for (uint32_t i = 0; i < computations; i++)
	{
		tim.start();

		if (FS_est < OPT_FS_BARRIER)
		{
			if (mode == WORKLOAD_COUNT)
				result = udFS_wc(R, S);
			else if (mode == XOR)
				result = udFS_xor(R, S);
			else if (mode == SIMD_XOR)
				result = udFS_simd_xor(R, S);
		}
		else
		{
			if (mode == WORKLOAD_COUNT)
				result = bgudFS_wc(R, S, max_group_size, BIR, BIS);
			else if (mode == XOR)
				result = bgudFS_xor(R, S, max_group_size, BIR, BIS);
			else if (mode == SIMD_XOR)
				result = bgudFS_simd_xor(R, S, max_group_size, BIR, BIS);
		}

		joinTime = tim.stop();
		printf("Joining:                    %lf secs\n", joinTime);
	}
	printf("------------------------");

	return result;
}
