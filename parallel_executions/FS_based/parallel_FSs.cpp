#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"
#include "../../containers/FS_map.hpp"

// sorting conditions
bool CompareByStart(const Record& a, const Record& b);
bool CompareByLoad(const FSGrouped& a, const FSGrouped& b);

// function to estimate FSs extent
uint64_t FS_estimation(Relation& R, Relation& S);

// direct result discovery functions
void   udFS_direct_discovery_count(Relation &R, Relation &S, FSMapDetached* fsmd, uint32_t numThreads);
void   udFS_direct_discovery_set  (Relation &R, Relation &S, FSMapDetached* fsmd, uint32_t numThreads);
void bgudFS_direct_discovery_count(Relation &R, Relation &S, int max_group_size, BucketIndex& BIR, BucketIndex& BIS, FSMapGrouped* fsmg, uint32_t numThreads);
void bgudFS_direct_discovery_set  (Relation &R, Relation &S, int max_group_size, BucketIndex& BIR, BucketIndex& BIS, FSMapGrouped* fsmg, uint32_t numThreads);

// intermediate result discovery functions
void SetBoundaries(Relation& R, Relation& S, uint32_t numPartitions, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t* partition_sizes);
void   udFS_intermediate_discovery_count(Relation &R, Relation &S, FSMapDetached* fsmd_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2);
void   udFS_intermediate_discovery_set  (Relation &R, Relation &S, FSMapDetached* fsmd_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2);
void   udFS_intermediate_discovery_split_count(FSMapDetached* fsmd_intermediate, FSMapDetached* fsmd, uint32_t numThreads);
void   udFS_intermediate_discovery_split_set  (FSMapDetached* fsmd_intermediate, FSMapDetached* fsmd, uint32_t numThreads, Relation& R, Relation& S, Relation* R_copies, Relation* S_copies, int allocationStrategy, int* thread_core_map);
void bgudFS_intermediate_discovery_count(Relation &R, Relation &S, int max_group_size,                                     FSMapGrouped* fsmg_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2);
void bgudFS_intermediate_discovery_set  (Relation &R, Relation &S, int max_group_size, BucketIndex& BIR, BucketIndex& BIS, FSMapGrouped* fsmg_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2);
void bgudFS_intermediate_discovery_split_count(FSMapGrouped* fsmg_intermediate, FSMapGrouped* fsmg, uint32_t numThreads);
void bgudFS_intermediate_discovery_split_set  (FSMapGrouped* fsmg_intermediate, FSMapGrouped* fsmg, uint32_t numThreads, Relation& R, Relation& S, Relation* R_copies, Relation* S_copies, int allocationStrategy, int* thread_core_map);

// direct execution functions
uint64_t udFS_internalLoop_wc       ( Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);
uint64_t udFS_internalLoop_xor      ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);
uint64_t udFS_internalLoop_simd_xor ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);

int stick_this_thread_to_core(int core_id);
int get_closest_node(int core_id);
int get_most_distant_node(int core_id);

// thread functions
struct StructForParallelDetachedFSs
{
	FSMapDetached* fsmd;

	uint32_t thread_id;
	uint32_t core_id;

	int allocationStrategy;

	uint64_t* result;
};

struct StructForParallelGroupedFSs
{
	FSMapGrouped* fsmg;

	uint32_t thread_id;
	uint32_t core_id;

	int allocationStrategy;

	uint64_t* result;
};

struct StructForParallelDirectExecution
{
	Timestamp* r_start;
	Timestamp* s_start;
	Timestamp* r_end;
	Timestamp* s_end;
	Timestamp* lastR;
	Timestamp* lastS;
	Timestamp* total_lastR;
	Timestamp* total_lastS;

	uint32_t thread_id;
	uint32_t core_id;

	int allocationStrategy;

	uint64_t* result;
};

void* detached_wc(void* args)
{
	StructForParallelDetachedFSs* spdfs = (StructForParallelDetachedFSs*) args;

	if (spdfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spdfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_wc( j+0 );
		res += spdfs->fsmd->compute_results_wc( j+1 );
		res += spdfs->fsmd->compute_results_wc( j+2 );
		res += spdfs->fsmd->compute_results_wc( j+3 );
		res += spdfs->fsmd->compute_results_wc( j+4 );
		res += spdfs->fsmd->compute_results_wc( j+5 );
		res += spdfs->fsmd->compute_results_wc( j+6 );
		res += spdfs->fsmd->compute_results_wc( j+7 );
		res += spdfs->fsmd->compute_results_wc( j+8 );
		res += spdfs->fsmd->compute_results_wc( j+9 );
		res += spdfs->fsmd->compute_results_wc( j+10 );
		res += spdfs->fsmd->compute_results_wc( j+11 );
		res += spdfs->fsmd->compute_results_wc( j+12 );
		res += spdfs->fsmd->compute_results_wc( j+13 );
		res += spdfs->fsmd->compute_results_wc( j+14 );
		res += spdfs->fsmd->compute_results_wc( j+15 );
		res += spdfs->fsmd->compute_results_wc( j+16 );
		res += spdfs->fsmd->compute_results_wc( j+17 );
		res += spdfs->fsmd->compute_results_wc( j+18 );
		res += spdfs->fsmd->compute_results_wc( j+19 );
		res += spdfs->fsmd->compute_results_wc( j+20 );
		res += spdfs->fsmd->compute_results_wc( j+21 );
		res += spdfs->fsmd->compute_results_wc( j+22 );
		res += spdfs->fsmd->compute_results_wc( j+23 );
		res += spdfs->fsmd->compute_results_wc( j+24 );
		res += spdfs->fsmd->compute_results_wc( j+25 );
		res += spdfs->fsmd->compute_results_wc( j+26 );
		res += spdfs->fsmd->compute_results_wc( j+27 );
		res += spdfs->fsmd->compute_results_wc( j+28 );
		res += spdfs->fsmd->compute_results_wc( j+29 );
		res += spdfs->fsmd->compute_results_wc( j+30 );
		res += spdfs->fsmd->compute_results_wc( j+31 );

		j += 32;
	}

	while (j < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_wc( j );

		j++;
	}

	*(spdfs->result) = res;

	return NULL;
}

void* grouped_wc(void* args)
{
	StructForParallelGroupedFSs* spgfs = (StructForParallelGroupedFSs*) args;

	if (spgfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spgfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_wc( j+0 );
		res += spgfs->fsmg->compute_results_wc( j+1 );
		res += spgfs->fsmg->compute_results_wc( j+2 );
		res += spgfs->fsmg->compute_results_wc( j+3 );
		res += spgfs->fsmg->compute_results_wc( j+4 );
		res += spgfs->fsmg->compute_results_wc( j+5 );
		res += spgfs->fsmg->compute_results_wc( j+6 );
		res += spgfs->fsmg->compute_results_wc( j+7 );
		res += spgfs->fsmg->compute_results_wc( j+8 );
		res += spgfs->fsmg->compute_results_wc( j+9 );
		res += spgfs->fsmg->compute_results_wc( j+10 );
		res += spgfs->fsmg->compute_results_wc( j+11 );
		res += spgfs->fsmg->compute_results_wc( j+12 );
		res += spgfs->fsmg->compute_results_wc( j+13 );
		res += spgfs->fsmg->compute_results_wc( j+14 );
		res += spgfs->fsmg->compute_results_wc( j+15 );
		res += spgfs->fsmg->compute_results_wc( j+16 );
		res += spgfs->fsmg->compute_results_wc( j+17 );
		res += spgfs->fsmg->compute_results_wc( j+18 );
		res += spgfs->fsmg->compute_results_wc( j+19 );
		res += spgfs->fsmg->compute_results_wc( j+20 );
		res += spgfs->fsmg->compute_results_wc( j+21 );
		res += spgfs->fsmg->compute_results_wc( j+22 );
		res += spgfs->fsmg->compute_results_wc( j+23 );
		res += spgfs->fsmg->compute_results_wc( j+24 );
		res += spgfs->fsmg->compute_results_wc( j+25 );
		res += spgfs->fsmg->compute_results_wc( j+26 );
		res += spgfs->fsmg->compute_results_wc( j+27 );
		res += spgfs->fsmg->compute_results_wc( j+28 );
		res += spgfs->fsmg->compute_results_wc( j+29 );
		res += spgfs->fsmg->compute_results_wc( j+30 );
		res += spgfs->fsmg->compute_results_wc( j+31 );

		j += 32;
	}

	while (j < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_wc( j );

		j++;
	}

	*(spgfs->result) = res;

	return NULL;
}

void* detached_xor(void* args)
{
	StructForParallelDetachedFSs* spdfs = (StructForParallelDetachedFSs*) args;

	if (spdfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spdfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_xor( j+0 );
		res += spdfs->fsmd->compute_results_xor( j+1 );
		res += spdfs->fsmd->compute_results_xor( j+2 );
		res += spdfs->fsmd->compute_results_xor( j+3 );
		res += spdfs->fsmd->compute_results_xor( j+4 );
		res += spdfs->fsmd->compute_results_xor( j+5 );
		res += spdfs->fsmd->compute_results_xor( j+6 );
		res += spdfs->fsmd->compute_results_xor( j+7 );
		res += spdfs->fsmd->compute_results_xor( j+8 );
		res += spdfs->fsmd->compute_results_xor( j+9 );
		res += spdfs->fsmd->compute_results_xor( j+10 );
		res += spdfs->fsmd->compute_results_xor( j+11 );
		res += spdfs->fsmd->compute_results_xor( j+12 );
		res += spdfs->fsmd->compute_results_xor( j+13 );
		res += spdfs->fsmd->compute_results_xor( j+14 );
		res += spdfs->fsmd->compute_results_xor( j+15 );
		res += spdfs->fsmd->compute_results_xor( j+16 );
		res += spdfs->fsmd->compute_results_xor( j+17 );
		res += spdfs->fsmd->compute_results_xor( j+18 );
		res += spdfs->fsmd->compute_results_xor( j+19 );
		res += spdfs->fsmd->compute_results_xor( j+20 );
		res += spdfs->fsmd->compute_results_xor( j+21 );
		res += spdfs->fsmd->compute_results_xor( j+22 );
		res += spdfs->fsmd->compute_results_xor( j+23 );
		res += spdfs->fsmd->compute_results_xor( j+24 );
		res += spdfs->fsmd->compute_results_xor( j+25 );
		res += spdfs->fsmd->compute_results_xor( j+26 );
		res += spdfs->fsmd->compute_results_xor( j+27 );
		res += spdfs->fsmd->compute_results_xor( j+28 );
		res += spdfs->fsmd->compute_results_xor( j+29 );
		res += spdfs->fsmd->compute_results_xor( j+30 );
		res += spdfs->fsmd->compute_results_xor( j+31 );

		j += 32;
	}

	while (j < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_xor( j );

		j++;
	}

	*(spdfs->result) = res;

	return NULL;
}

void* grouped_xor(void* args)
{
	StructForParallelGroupedFSs* spgfs = (StructForParallelGroupedFSs*) args;

	if (spgfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spgfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

//Timer tim;
//tim.start();

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_xor( j+0 );
		res += spgfs->fsmg->compute_results_xor( j+1 );
		res += spgfs->fsmg->compute_results_xor( j+2 );
		res += spgfs->fsmg->compute_results_xor( j+3 );
		res += spgfs->fsmg->compute_results_xor( j+4 );
		res += spgfs->fsmg->compute_results_xor( j+5 );
		res += spgfs->fsmg->compute_results_xor( j+6 );
		res += spgfs->fsmg->compute_results_xor( j+7 );
		res += spgfs->fsmg->compute_results_xor( j+8 );
		res += spgfs->fsmg->compute_results_xor( j+9 );
		res += spgfs->fsmg->compute_results_xor( j+10 );
		res += spgfs->fsmg->compute_results_xor( j+11 );
		res += spgfs->fsmg->compute_results_xor( j+12 );
		res += spgfs->fsmg->compute_results_xor( j+13 );
		res += spgfs->fsmg->compute_results_xor( j+14 );
		res += spgfs->fsmg->compute_results_xor( j+15 );
		res += spgfs->fsmg->compute_results_xor( j+16 );
		res += spgfs->fsmg->compute_results_xor( j+17 );
		res += spgfs->fsmg->compute_results_xor( j+18 );
		res += spgfs->fsmg->compute_results_xor( j+19 );
		res += spgfs->fsmg->compute_results_xor( j+20 );
		res += spgfs->fsmg->compute_results_xor( j+21 );
		res += spgfs->fsmg->compute_results_xor( j+22 );
		res += spgfs->fsmg->compute_results_xor( j+23 );
		res += spgfs->fsmg->compute_results_xor( j+24 );
		res += spgfs->fsmg->compute_results_xor( j+25 );
		res += spgfs->fsmg->compute_results_xor( j+26 );
		res += spgfs->fsmg->compute_results_xor( j+27 );
		res += spgfs->fsmg->compute_results_xor( j+28 );
		res += spgfs->fsmg->compute_results_xor( j+29 );
		res += spgfs->fsmg->compute_results_xor( j+30 );
		res += spgfs->fsmg->compute_results_xor( j+31 );

		j += 32;
	}

	while (j < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_xor( j );

		j++;
	}

	*(spgfs->result) = res;

//double thread_time = tim.stop();
//printf("thread %d: %lf\n", spgfs->thread_id, thread_time);

	return NULL;
}

void* detached_simd_xor(void* args)
{
	StructForParallelDetachedFSs* spdfs = (StructForParallelDetachedFSs*) args;

	if (spdfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spdfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_simd_xor( j+0 );
		res += spdfs->fsmd->compute_results_simd_xor( j+1 );
		res += spdfs->fsmd->compute_results_simd_xor( j+2 );
		res += spdfs->fsmd->compute_results_simd_xor( j+3 );
		res += spdfs->fsmd->compute_results_simd_xor( j+4 );
		res += spdfs->fsmd->compute_results_simd_xor( j+5 );
		res += spdfs->fsmd->compute_results_simd_xor( j+6 );
		res += spdfs->fsmd->compute_results_simd_xor( j+7 );
		res += spdfs->fsmd->compute_results_simd_xor( j+8 );
		res += spdfs->fsmd->compute_results_simd_xor( j+9 );
		res += spdfs->fsmd->compute_results_simd_xor( j+10 );
		res += spdfs->fsmd->compute_results_simd_xor( j+11 );
		res += spdfs->fsmd->compute_results_simd_xor( j+12 );
		res += spdfs->fsmd->compute_results_simd_xor( j+13 );
		res += spdfs->fsmd->compute_results_simd_xor( j+14 );
		res += spdfs->fsmd->compute_results_simd_xor( j+15 );
		res += spdfs->fsmd->compute_results_simd_xor( j+16 );
		res += spdfs->fsmd->compute_results_simd_xor( j+17 );
		res += spdfs->fsmd->compute_results_simd_xor( j+18 );
		res += spdfs->fsmd->compute_results_simd_xor( j+19 );
		res += spdfs->fsmd->compute_results_simd_xor( j+20 );
		res += spdfs->fsmd->compute_results_simd_xor( j+21 );
		res += spdfs->fsmd->compute_results_simd_xor( j+22 );
		res += spdfs->fsmd->compute_results_simd_xor( j+23 );
		res += spdfs->fsmd->compute_results_simd_xor( j+24 );
		res += spdfs->fsmd->compute_results_simd_xor( j+25 );
		res += spdfs->fsmd->compute_results_simd_xor( j+26 );
		res += spdfs->fsmd->compute_results_simd_xor( j+27 );
		res += spdfs->fsmd->compute_results_simd_xor( j+28 );
		res += spdfs->fsmd->compute_results_simd_xor( j+29 );
		res += spdfs->fsmd->compute_results_simd_xor( j+30 );
		res += spdfs->fsmd->compute_results_simd_xor( j+31 );

		j += 32;
	}

	while (j < spdfs->fsmd->numResults)
	{
		res += spdfs->fsmd->compute_results_simd_xor( j );

		j++;
	}

	*(spdfs->result) = res;

	return NULL;
}

void* grouped_simd_xor(void* args)
{
	StructForParallelGroupedFSs* spgfs = (StructForParallelGroupedFSs*) args;

	if (spgfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spgfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	while ((j+32) < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_simd_xor( j+0 );
		res += spgfs->fsmg->compute_results_simd_xor( j+1 );
		res += spgfs->fsmg->compute_results_simd_xor( j+2 );
		res += spgfs->fsmg->compute_results_simd_xor( j+3 );
		res += spgfs->fsmg->compute_results_simd_xor( j+4 );
		res += spgfs->fsmg->compute_results_simd_xor( j+5 );
		res += spgfs->fsmg->compute_results_simd_xor( j+6 );
		res += spgfs->fsmg->compute_results_simd_xor( j+7 );
		res += spgfs->fsmg->compute_results_simd_xor( j+8 );
		res += spgfs->fsmg->compute_results_simd_xor( j+9 );
		res += spgfs->fsmg->compute_results_simd_xor( j+10 );
		res += spgfs->fsmg->compute_results_simd_xor( j+11 );
		res += spgfs->fsmg->compute_results_simd_xor( j+12 );
		res += spgfs->fsmg->compute_results_simd_xor( j+13 );
		res += spgfs->fsmg->compute_results_simd_xor( j+14 );
		res += spgfs->fsmg->compute_results_simd_xor( j+15 );
		res += spgfs->fsmg->compute_results_simd_xor( j+16 );
		res += spgfs->fsmg->compute_results_simd_xor( j+17 );
		res += spgfs->fsmg->compute_results_simd_xor( j+18 );
		res += spgfs->fsmg->compute_results_simd_xor( j+19 );
		res += spgfs->fsmg->compute_results_simd_xor( j+20 );
		res += spgfs->fsmg->compute_results_simd_xor( j+21 );
		res += spgfs->fsmg->compute_results_simd_xor( j+22 );
		res += spgfs->fsmg->compute_results_simd_xor( j+23 );
		res += spgfs->fsmg->compute_results_simd_xor( j+24 );
		res += spgfs->fsmg->compute_results_simd_xor( j+25 );
		res += spgfs->fsmg->compute_results_simd_xor( j+26 );
		res += spgfs->fsmg->compute_results_simd_xor( j+27 );
		res += spgfs->fsmg->compute_results_simd_xor( j+28 );
		res += spgfs->fsmg->compute_results_simd_xor( j+29 );
		res += spgfs->fsmg->compute_results_simd_xor( j+30 );
		res += spgfs->fsmg->compute_results_simd_xor( j+31 );

		j += 32;
	}

	while (j < spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_simd_xor( j );

		j++;
	}

	*(spgfs->result) = res;

	return NULL;
}

void* grouped_rectangle_simd_xor(void* args)
{
	StructForParallelGroupedFSs* spgfs = (StructForParallelGroupedFSs*) args;

	if (spgfs->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spgfs->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	uint32_t j = 0;
	uint32_t batch_size = 500; // the number of group scans to perform in a rectangle
	while ((j + batch_size) <= spgfs->fsmg->numResults)
	{
		res += spgfs->fsmg->compute_results_rectangle_simd_xor(j, batch_size);

		j += batch_size;
	}

	batch_size = spgfs->fsmg->numResults - j;
	if (j < spgfs->fsmg->numResults)
		res += spgfs->fsmg->compute_results_rectangle_simd_xor(j, batch_size);

	*(spgfs->result) = res;

	return NULL;
}

void* direct_execution_wc(void* args)
{
	StructForParallelDirectExecution* spde = (StructForParallelDirectExecution*) args;

	if (spde->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spde->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	while ((spde->r_start < spde->lastR) && (spde->s_start < spde->lastS))
	{
		if (*(spde->r_start) < *(spde->s_start) )
		{
			res += udFS_internalLoop_wc( spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		else
		{
			res += udFS_internalLoop_wc( spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	if ( (spde->lastS != spde->total_lastS) && (spde->lastR != spde->total_lastR) )
	{
		while (spde->r_start < spde->lastR)
		{
			res += udFS_internalLoop_wc( spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		while (spde->s_start < spde->lastS)
		{
			res += udFS_internalLoop_wc( spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	*(spde->result) = res;

	return NULL;
}

void* direct_execution_xor(void* args)
{
	StructForParallelDirectExecution* spde = (StructForParallelDirectExecution*) args;

	if (spde->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spde->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	while ((spde->r_start < spde->lastR) && (spde->s_start < spde->lastS))
	{
		if (*(spde->r_start) < *(spde->s_start) )
		{
			res += udFS_internalLoop_xor( spde->r_start, spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		else
		{
			res += udFS_internalLoop_xor( spde->s_start, spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	if ( (spde->lastS != spde->total_lastS) && (spde->lastR != spde->total_lastR) )
	{
		while (spde->r_start < spde->lastR)
		{
			res += udFS_internalLoop_xor( spde->r_start, spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		while (spde->s_start < spde->lastS)
		{
			res += udFS_internalLoop_xor( spde->s_start, spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	*(spde->result) = res;

	return NULL;
}

void* direct_execution_simd_xor(void* args)
{
	StructForParallelDirectExecution* spde = (StructForParallelDirectExecution*) args;

	if (spde->allocationStrategy != OS)
	{
		if ( stick_this_thread_to_core(spde->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	uint64_t res = 0;

	while ((spde->r_start < spde->lastR) && (spde->s_start < spde->lastS))
	{
		if (*(spde->r_start) < *(spde->s_start) )
		{
			res += udFS_internalLoop_simd_xor( spde->r_start, spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		else
		{
			res += udFS_internalLoop_simd_xor( spde->s_start, spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	if ( (spde->lastS != spde->total_lastS) && (spde->lastR != spde->total_lastR) )
	{
		while (spde->r_start < spde->lastR)
		{
			res += udFS_internalLoop_simd_xor( spde->r_start, spde->r_end, spde->s_start, spde->total_lastS);

			(spde->r_start)++;
			(spde->r_end)++;
		}
		while (spde->s_start < spde->lastS)
		{
			res += udFS_internalLoop_simd_xor( spde->s_start, spde->s_end, spde->r_start, spde->total_lastR);

			(spde->s_start)++;
			(spde->s_end)++;
		}
	}

	*(spde->result) = res;

	return NULL;
}

uint64_t parallel_FSs(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map)
{
	Timer tim;
	BucketIndex BIR, BIS;
	uint64_t FS_est;
	uint64_t result;

	// Sort
	printf("-------- PRE-JOIN --------\n");
	tim.start();
	std::span<Record> tempR( R.record_list, R.numRecords);
	std::span<Record> tempS( S.record_list, S.numRecords);
	oneapi::tbb::task_arena arenaA(numThreads);
	oneapi::tbb::task_arena arenaB(numThreads);
	arenaA.execute([&]{ oneapi::tbb::parallel_sort( tempR.begin(), tempR.end() , CompareByStart); });
	arenaA.execute([&]{ oneapi::tbb::parallel_sort( tempS.begin(), tempS.end() , CompareByStart); });
	double timeSorting = tim.stop();
	printf("Sorting:                    %lf secs\n", timeSorting);

	// column store switch
	tim.start();
	Relation R_copies[numa_max_node() + 1];
	Relation S_copies[numa_max_node() + 1];
	R.allocationStrategy = OS;
	S.allocationStrategy = OS;
	R.columnStoreSwitch(thread_core_map[0]);
	S.columnStoreSwitch(thread_core_map[0]);
	if (allocationStrategy != OS)
	{
		for (uint32_t i = 0; i < (numa_max_node() + 1); i++)
		{
			R_copies[i].record_list = NULL;
			R_copies[i].startpoints = R.numRecords ? (Timestamp*) numa_alloc_onnode(R.numRecords*sizeof(Timestamp), i) : NULL;
			R_copies[i].endpoints   = R.numRecords ? (Timestamp*) numa_alloc_onnode(R.numRecords*sizeof(Timestamp), i) : NULL;
			for (uint32_t j = 0; j < R.numRecords; j++)
			{
				R_copies[i].startpoints[j] = R.startpoints[j];
				R_copies[i].endpoints[j]   = R.endpoints[j];
			}
			R_copies[i].numRecords = R.numRecords;
			R_copies[i].minStart = R_copies[i].startpoints[0];
			R_copies[i].maxStart = R_copies[i].startpoints[R_copies[i].numRecords - 1];
			R_copies[i].allocationStrategy = allocationStrategy;

			S_copies[i].record_list = NULL;
			S_copies[i].startpoints = S.numRecords ? (Timestamp*) numa_alloc_onnode(S.numRecords*sizeof(Timestamp), i) : NULL;
			S_copies[i].endpoints   = S.numRecords ? (Timestamp*) numa_alloc_onnode(S.numRecords*sizeof(Timestamp), i) : NULL;
			for (uint32_t j = 0; j < S.numRecords; j++)
			{
				S_copies[i].startpoints[j] = S.startpoints[j];
				S_copies[i].endpoints[j]   = S.endpoints[j];
			}
			S_copies[i].numRecords = S.numRecords;
			S_copies[i].minStart = S_copies[i].startpoints[0];
			S_copies[i].maxStart = S_copies[i].startpoints[S_copies[i].numRecords - 1];
			S_copies[i].allocationStrategy = allocationStrategy;
		}
	}
	else
	{
		for (uint32_t i = 0; i < (numa_max_node() + 1); i++)
		{
			R_copies[i].allocationStrategy = OS;
			R_copies[i].record_list = NULL;
			R_copies[i].startpoints = NULL;
			R_copies[i].endpoints   = NULL;

			S_copies[i].allocationStrategy = OS;
			S_copies[i].record_list = NULL;
			S_copies[i].startpoints = NULL;
			S_copies[i].endpoints   = NULL;
		}
	}
	double switchingTime = tim.stop();
	printf("Switching to Column Store:  %lf secs\n", switchingTime);

	// selectivity check
	tim.start();
	FS_est = FS_estimation( R, S);
	double selectivityEstimationTime = tim.stop();
	printf("Selectivity Estimation:     %lf secs ", selectivityEstimationTime);

	// buckets (if needed)
	double bucketBuildingTime = 0;
	if (FS_est < OPT_FS_BARRIER)
	{
		printf("* calculated %ld, going for udFS *\n", FS_est);
	}
	else
	{
		printf("* calculated %ld, going for bgudFS *\n", FS_est);
		tim.start();
		BIR.build( R, 1000);
		BIS.build( S, 1000);
		bucketBuildingTime = tim.stop();
		printf("Bucketing:                  %lf secs\n", bucketBuildingTime);
	}

	if (FS_est >= OPT_FS_BARRIER)
	{
		// Assigning Forward Scans
		// keeping FS Map for udFS and direct version for partitioning even though we never use them
		bool directVersion = false;
		tim.start();
		FSMapDetached fsmd[numThreads];
		FSMapGrouped  fsmg[numThreads];
		if (directVersion)
		{
			// implemented only for OS allocation, to show its inferiority against intermediate FS map
			if (FS_est < OPT_FS_BARRIER)
			{
				udFS_direct_discovery_count( R, S, fsmd, numThreads );
				udFS_direct_discovery_set( R, S, fsmd, numThreads );
			}
			else
			{
				bgudFS_direct_discovery_count( R, S, max_group_size, BIR, BIS, fsmg, numThreads );
				bgudFS_direct_discovery_set( R, S, max_group_size, BIR, BIS, fsmg, numThreads );
			}
		}
		else
		{
			// create boundaries
			uint32_t numPartitions = numThreads;
			uint32_t* partition_sizes = (uint32_t*) malloc( numPartitions   * sizeof(uint32_t) );
			Timestamp** boundariesR = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
			Timestamp** boundariesS = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
			SetBoundaries( R, S, numPartitions, boundariesR, boundariesS, partition_sizes);

			uint32_t points_to_write1[numThreads];
			uint32_t points_to_write2[numThreads];
			if (FS_est < OPT_FS_BARRIER)
			{
				// never used, inferior to non-FSMap direct execution
				FSMapDetached fsmd_intermediate[2];
				udFS_intermediate_discovery_count( R, S, fsmd_intermediate, boundariesR, boundariesS, numThreads, points_to_write1, points_to_write2);
				udFS_intermediate_discovery_set  ( R, S, fsmd_intermediate, boundariesR, boundariesS, numThreads, points_to_write1, points_to_write2);

				udFS_intermediate_discovery_split_count( fsmd_intermediate, fsmd, numThreads);
				udFS_intermediate_discovery_split_set  ( fsmd_intermediate, fsmd, numThreads, R, S, R_copies, S_copies, allocationStrategy, thread_core_map);
			}
			else
			{
				FSMapGrouped  fsmg_intermediate[2];
				bgudFS_intermediate_discovery_count( R, S, max_group_size,           fsmg_intermediate, boundariesR, boundariesS, numThreads, points_to_write1, points_to_write2);
				bgudFS_intermediate_discovery_set  ( R, S, max_group_size, BIR, BIS, fsmg_intermediate, boundariesR, boundariesS, numThreads, points_to_write1, points_to_write2);

				bgudFS_intermediate_discovery_split_count( fsmg_intermediate, fsmg, numThreads);
				bgudFS_intermediate_discovery_split_set  ( fsmg_intermediate, fsmg, numThreads, R, S, R_copies, S_copies, allocationStrategy, thread_core_map);
			}

			// free memory used
			free(boundariesR);
			free(boundariesS);
			free(partition_sizes);
		}
		double partitioningTime = tim.stop();
		printf("Partitioning Time:          %lf secs\n", partitioningTime);
		printf(" Pre-Join Time:             %lf secs\n", timeSorting + switchingTime + selectivityEstimationTime + bucketBuildingTime + partitioningTime);
		printf("--------------------------\n");

		// parallel join
		printf("\n--------- JOIN ---------\n");
		double joinTime;
		StructForParallelDetachedFSs spdfs[numThreads];
		StructForParallelGroupedFSs  spgfs[numThreads];
		pthread_t thread_id[numThreads];
		uint64_t thread_results[numThreads];
		for (uint32_t c = 0; c < computations; c++)
		{
			tim.start();
			result = 0;

			if (FS_est < OPT_FS_BARRIER)
			{
				for (uint32_t i = 0; i < numThreads; i++)
				{
					spdfs[i].fsmd = &fsmd[i];

					spdfs[i].thread_id = i;
					spdfs[i].core_id = thread_core_map[i];

					spdfs[i].allocationStrategy = allocationStrategy;

					spdfs[i].result = &thread_results[i];

					if (mode == WORKLOAD_COUNT)
						pthread_create( &thread_id[i], NULL, detached_wc, (void*) &spdfs[i]);
					else if (mode == XOR)
						pthread_create( &thread_id[i], NULL, detached_xor, (void*) &spdfs[i]);
					else if (mode == SIMD_XOR)
						pthread_create( &thread_id[i], NULL, detached_simd_xor, (void*) &spdfs[i]);
				}
				for (uint32_t i = 0; i < numThreads; i++)
				{
					pthread_join( thread_id[i], NULL);
				}
			}
			else
			{
				for (uint32_t i = 0; i < numThreads; i++)
				{
					spgfs[i].fsmg = &fsmg[i];

					spgfs[i].thread_id = i;
					spgfs[i].core_id = thread_core_map[i];

					spgfs[i].allocationStrategy = allocationStrategy;

					spgfs[i].result = &thread_results[i];

					if (mode == WORKLOAD_COUNT)
						pthread_create( &thread_id[i], NULL, grouped_wc, (void*) &spgfs[i]);
					else if (mode == XOR)
						pthread_create( &thread_id[i], NULL, grouped_xor, (void*) &spgfs[i]);
					else if (mode == SIMD_XOR)
						pthread_create( &thread_id[i], NULL, grouped_simd_xor, (void*) &spgfs[i]);
						//pthread_create( &thread_id[i], NULL, grouped_rectangle_simd_xor, (void*) &spgfs[i]);
				}
				for (uint32_t i = 0; i < numThreads; i++)
				{
					pthread_join( thread_id[i], NULL);
				}
			}

			// add thread results
			for (uint32_t i = 0; i < numThreads; i++)
			{
				result += thread_results[i];
			}

			joinTime = tim.stop();
			printf("Joining:                    %lf secs\n", joinTime);
		}
		printf("------------------------");
	}
	else
	{
		// create boundaries
		tim.start();
		uint32_t numPartitions = numThreads;
		uint32_t* partition_sizes = (uint32_t*) malloc( numPartitions   * sizeof(uint32_t) );
		Timestamp** boundariesR = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
		Timestamp** boundariesS = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
		SetBoundaries( R, S, numPartitions, boundariesR, boundariesS, partition_sizes);
		double boundariesTime = tim.stop();
		printf("Borders time:               %lf secs\n", boundariesTime);

		printf(" Pre-Join Time:             %lf secs\n", timeSorting + switchingTime + selectivityEstimationTime + boundariesTime);
		printf("--------------------------\n");

		// numa functions take time, so use a numa_map to call them less frequently
		int numa_map[numThreads];
		if (allocationStrategy == CLOSEST)
		{
			for (uint32_t i = 0; i < numThreads; i++)
				numa_map[i] = get_closest_node( thread_core_map[i] );
		}
		else if (allocationStrategy == OPPOSITE)
		{
			for (uint32_t i = 0; i < numThreads; i++)
				numa_map[i] = get_most_distant_node( thread_core_map[i] );
		}

		// direct execution
		StructForParallelDirectExecution spde[numThreads];
		pthread_t thread_id[numThreads];
		uint64_t thread_results[numThreads];

		printf("\n--------- JOIN ---------\n");
		double joinTime;
		for (uint32_t c = 0; c < computations; c++)
		{
			tim.start();
			result = 0;

			for (uint32_t i = 0; i < numThreads; i++)
			{
				if (allocationStrategy != OS)
				{
					spde[i].r_start     = R_copies[numa_map[i]].startpoints + ((boundariesR[i] + 1) - R.startpoints);
					spde[i].s_start     = S_copies[numa_map[i]].startpoints + ((boundariesS[i] + 1) - S.startpoints);
					spde[i].r_end       = R_copies[numa_map[i]].endpoints   + ((boundariesR[i] + 1) - R.startpoints);
					spde[i].s_end       = S_copies[numa_map[i]].endpoints   + ((boundariesS[i] + 1) - S.startpoints);
					spde[i].lastR       = R_copies[numa_map[i]].startpoints + ((boundariesR[i+1] + 1) - R.startpoints);
					spde[i].lastS       = S_copies[numa_map[i]].startpoints + ((boundariesS[i+1] + 1) - S.startpoints);
					spde[i].total_lastR = R_copies[numa_map[i]].startpoints + ((boundariesR[numThreads] + 1) - R.startpoints);
					spde[i].total_lastS = S_copies[numa_map[i]].startpoints + ((boundariesS[numThreads] + 1) - S.startpoints);
				}
				else
				{
					spde[i].r_start = boundariesR[i] + 1;
					spde[i].s_start = boundariesS[i] + 1;
					spde[i].r_end   = R.endpoints + (spde[i].r_start - R.startpoints);
					spde[i].s_end   = S.endpoints + (spde[i].s_start - S.startpoints);
					spde[i].lastR   = boundariesR[i+1] + 1;
					spde[i].lastS   = boundariesS[i+1] + 1;
					spde[i].total_lastR = boundariesR[numThreads] + 1;
					spde[i].total_lastS = boundariesS[numThreads] + 1;
				}

				spde[i].thread_id = i;
				spde[i].core_id = thread_core_map[i];

				spde[i].allocationStrategy = allocationStrategy;

				spde[i].result = &thread_results[i];

				if (mode == WORKLOAD_COUNT)
					pthread_create( &thread_id[i], NULL, direct_execution_wc, (void*) &spde[i]);
				else if (mode == XOR)
					pthread_create( &thread_id[i], NULL, direct_execution_xor, (void*) &spde[i]);
				else if (mode == SIMD_XOR)
					pthread_create( &thread_id[i], NULL, direct_execution_simd_xor, (void*) &spde[i]);
			}
			for (uint32_t i = 0; i < numThreads; i++)
			{
				pthread_join( thread_id[i], NULL);
			}

			// add thread results
			for (uint32_t i = 0; i < numThreads; i++)
			{
				result += thread_results[i];
			}

			joinTime = tim.stop();
			printf("Joining:                    %lf secs\n", joinTime);
		}
		printf("------------------------");

		// free memory used
		free(boundariesR);
		free(boundariesS);
		free(partition_sizes);
	}

	return result;
}
