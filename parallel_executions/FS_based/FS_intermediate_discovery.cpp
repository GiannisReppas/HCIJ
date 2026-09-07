#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"
#include "../../containers/FS_map.hpp"

// internal loops defined at the end of file
void       udFS_intermediate_discovery_set_internalLoop  ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd_intermediate, uint32_t& points_to_write);
void     bgudFS_intermediate_discovery_set_internalLoop  ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg_intermediate, uint32_t& points_to_write);

// sorting condition for grouping optimization
static bool CompareByEndPointersT( Timestamp*& a, Timestamp*& b);

int get_closest_node(int core_id);
int get_most_distant_node(int core_id);

////////////////////
//// Discoveries ///
////////////////////

struct StructForParallelDiscoveryCount
{
	Timestamp* r_start;
	Timestamp* s_start;
	Timestamp* r_end;
	Timestamp* s_end;
	Timestamp* lastR;
	Timestamp* lastS;
	Timestamp* total_lastR;
	Timestamp* total_lastS;
	int max_group_size;
	BucketIndex* BIR;
	BucketIndex* BIS;

	uint32_t thread_id;
	uint32_t* points_to_write1;
	uint32_t* points_to_write2;
};

struct StructForParallelDiscoverySet
{
	Timestamp* r_start;
	Timestamp* s_start;
	Timestamp* r_end;
	Timestamp* s_end;
	Timestamp* lastR;
	Timestamp* lastS;
	Timestamp* total_lastR;
	Timestamp* total_lastS;
	int max_group_size;
	BucketIndex* BIR;
	BucketIndex* BIS;
	Timestamp* R_startpoints;
	Timestamp* S_startpoints;
	Timestamp* R_endpoints;
	Timestamp* S_endpoints;
	FSMapDetached* fsmd1;
	FSMapDetached* fsmd2;
	FSMapGrouped* fsmg1;
	FSMapGrouped* fsmg2;

	uint32_t thread_id;
	uint32_t* points_to_write1;
	uint32_t* points_to_write2;
};

void* udFS_intermediate_discovery_count_thread(void* args)
{
	StructForParallelDiscoveryCount* spdc = (StructForParallelDiscoveryCount*) args;

	uint32_t result1 = 0;
	uint32_t result2 = 0;

	while ((spdc->r_start < spdc->lastR) && (spdc->s_start < spdc->lastS))
	{
		if (*(spdc->r_start) < *(spdc->s_start) )
		{
			if (*(spdc->r_end) > *(spdc->s_start) )
				result1++;

			(spdc->r_start)++;
			(spdc->r_end)++;
		}
		else
		{
			if (*(spdc->s_end) > *(spdc->r_start) )
				result2++;

			(spdc->s_start)++;
			(spdc->s_end)++;
		}
	}

	if ( (spdc->lastS != spdc->total_lastS) && (spdc->lastR != spdc->total_lastR) )
	{
		while (spdc->r_start < spdc->lastR)
		{
			if (*(spdc->r_end) > *(spdc->s_start) )
				result1++;

			(spdc->r_start)++;
			(spdc->r_end)++;
		}
		while (spdc->s_start < spdc->lastS)
		{
			if (*(spdc->s_end) > *(spdc->r_start) )
				result2++;

			(spdc->s_start)++;
			(spdc->s_end)++;
		}
	}

	*(spdc->points_to_write1) = result1;
	*(spdc->points_to_write2) = result2;

	return NULL;
}

void udFS_intermediate_discovery_count(Relation &R, Relation &S, FSMapDetached* fsmd_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* result1, uint32_t* result2)
{
	uint32_t total_result1 = 0, total_result2 = 0;
	StructForParallelDiscoveryCount spdc[numThreads];
	pthread_t thread_id[numThreads];

	for (uint32_t i = 0; i < numThreads; i++)
	{
		spdc[i].r_start = boundariesR[i] + 1;
		spdc[i].s_start = boundariesS[i] + 1;
		spdc[i].r_end   = R.endpoints + (spdc[i].r_start - R.startpoints);
		spdc[i].s_end   = S.endpoints + (spdc[i].s_start - S.startpoints);
		spdc[i].lastR   = boundariesR[i+1] + 1;
		spdc[i].lastS   = boundariesS[i+1] + 1;
		spdc[i].total_lastR = boundariesR[numThreads] + 1;
		spdc[i].total_lastS = boundariesS[numThreads] + 1;

		spdc[i].thread_id = i;
		spdc[i].points_to_write1 = &result1[i];
		spdc[i].points_to_write2 = &result2[i];

		pthread_create( &thread_id[i], NULL, udFS_intermediate_discovery_count_thread, (void*) &spdc[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	uint32_t old_result;
	for (uint32_t i = 0; i < numThreads; i++)
	{
		old_result = total_result1;
		total_result1 += result1[i];
		result1[i] = old_result;
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		old_result = total_result2;
		total_result2 += result2[i];
		result2[i] = old_result;
	}

	fsmd_intermediate[0].result_list = (total_result1 != 0) ? (FSDetached*) malloc( total_result1 * sizeof(FSDetached) ) : NULL;
	fsmd_intermediate[0].numResults  = total_result1;

	fsmd_intermediate[1].result_list = (total_result2 != 0) ? (FSDetached*) malloc( total_result2 * sizeof(FSDetached) ) : NULL;
	fsmd_intermediate[1].numResults  = total_result2;
}

void* bgudFS_intermediate_discovery_count_thread(void* args)
{
	StructForParallelDiscoveryCount* spdc = (StructForParallelDiscoveryCount*) args;

	uint32_t result1 = 0;
	uint32_t result2 = 0;

	uint32_t group_size = 0;

	while ((spdc->r_start < spdc->lastR) && (spdc->s_start < spdc->lastS))
	{
		if (*(spdc->r_start) < *(spdc->s_start))
		{
			// Step 1: gather group for R.
			while ((spdc->r_start < spdc->lastR) && (*(spdc->r_start) < *(spdc->s_start)) && (group_size <= spdc->max_group_size) )
			{
				group_size++;
				(spdc->r_start)++;
				(spdc->r_end)++;
			}

			// just check if the biggest endpoint has at least one match (no need to check for previous group elements)
			if ( *(std::max_element( spdc->r_end - group_size, spdc->r_end)) > *(spdc->s_start) )
				result1++;

			// Step 3: empty current group.
			group_size = 0;
		}
		else
		{
			// Step 1: gather group for S.
			while ((spdc->s_start < spdc->lastS) && (*(spdc->r_start) >= *(spdc->s_start)) && (group_size <= spdc->max_group_size) )
			{
				group_size++;
				(spdc->s_start)++;
				(spdc->s_end)++;
			}

			// just check if the biggest endpoint has at least one match (no need to check for previous group elements)
			if ( *(std::max_element( spdc->s_end - group_size, spdc->s_end)) > *(spdc->r_start) )
				result2++;

			// Step 3: empty current group.
			group_size = 0;
		}
	}

	if ( (spdc->lastS != spdc->total_lastS) && (spdc->lastR != spdc->total_lastR) )
	{
		while (spdc->r_start < spdc->lastR)
		{
			// Step 1: gather group for R.
			while ((spdc->r_start < spdc->lastR) && (group_size <= spdc->max_group_size) )
			{
				group_size++;
				(spdc->r_start)++;
				(spdc->r_end)++;
			}

			// just check if the biggest endpoint has a match (no need to check for previous group elements)
			if ( *(std::max_element( spdc->r_end - group_size, spdc->r_end)) > *(spdc->s_start) )
				result1++;

			// Step 3: empty current group.
			group_size = 0;
		}
		while (spdc->s_start < spdc->lastS)
		{
			// Step 1: gather group for S.
			while ((spdc->s_start < spdc->lastS) && (group_size <= spdc->max_group_size) )
			{
				group_size++;
				(spdc->s_start)++;
				(spdc->s_end)++;
			}

			// just check if the biggest endpoint has a match (no need to check for previous group elements)
			if ( *(std::max_element( spdc->s_end - group_size, spdc->s_end)) > *(spdc->r_start) )
				result2++;

			// Step 3: empty current group.
			group_size = 0;
		}
	}

	*(spdc->points_to_write1) = result1;
	*(spdc->points_to_write2) = result2;

	return NULL;
}

void bgudFS_intermediate_discovery_count(Relation &R, Relation &S, int max_group_size, FSMapGrouped* fsmg_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* result1, uint32_t* result2)
{
	uint32_t total_result1 = 0;
	uint32_t total_result2 = 0;

	StructForParallelDiscoveryCount spdc[numThreads];
	pthread_t thread_id[numThreads];

	for (uint32_t i = 0; i < numThreads; i++)
	{
		spdc[i].r_start = boundariesR[i] + 1;
		spdc[i].s_start = boundariesS[i] + 1;
		spdc[i].r_end   = R.endpoints + (spdc[i].r_start - R.startpoints);
		spdc[i].s_end   = S.endpoints + (spdc[i].s_start - S.startpoints);
		spdc[i].lastR   = boundariesR[i+1] + 1;
		spdc[i].lastS   = boundariesS[i+1] + 1;
		spdc[i].total_lastR = boundariesR[numThreads] + 1;
		spdc[i].total_lastS = boundariesS[numThreads] + 1;
		spdc[i].max_group_size = max_group_size;

		spdc[i].thread_id = i;
		spdc[i].points_to_write1 = &result1[i];
		spdc[i].points_to_write2 = &result2[i];

		pthread_create( &thread_id[i], NULL, bgudFS_intermediate_discovery_count_thread, (void*) &spdc[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	uint32_t old_result;
	for (uint32_t i = 0; i < numThreads; i++)
	{
		old_result = total_result1;
		total_result1 += result1[i];
		result1[i] = old_result;
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		old_result = total_result2;
		total_result2 += result2[i];
		result2[i] = old_result;
	}

	fsmg_intermediate[0].result_list = (total_result1 != 0) ? (FSGrouped*) malloc( total_result1 * sizeof(FSGrouped) ) : NULL;
	fsmg_intermediate[0].numResults = total_result1;

	fsmg_intermediate[1].result_list = (total_result2 != 0) ? (FSGrouped*) malloc( total_result2 * sizeof(FSGrouped) ) : NULL;
	fsmg_intermediate[1].numResults = total_result2;
}

void* udFS_intermediate_discovery_set_thread(void* args)
{
	StructForParallelDiscoverySet* spds = (StructForParallelDiscoverySet*) args;

	uint32_t points_to_write1 = *(spds->points_to_write1);
	uint32_t points_to_write2 = *(spds->points_to_write2);

	while ((spds->r_start < spds->lastR) && (spds->s_start < spds->lastS))
	{
		if (*(spds->r_start) < *(spds->s_start))
		{
			udFS_intermediate_discovery_set_internalLoop( spds->r_start, spds->r_end, spds->s_start, spds->total_lastS, spds->fsmd1, points_to_write1);

			(spds->r_start)++;
			(spds->r_end)++;
		}
		else
		{
			udFS_intermediate_discovery_set_internalLoop( spds->s_start, spds->s_end, spds->r_start, spds->total_lastR, spds->fsmd2, points_to_write2);

			(spds->s_start)++;
			(spds->s_end)++;
		}
	}

	if ( (spds->lastS != spds->total_lastS) && (spds->lastR != spds->total_lastR) )
	{
		while (spds->r_start < spds->lastR)
		{
			udFS_intermediate_discovery_set_internalLoop( spds->r_start, spds->r_end, spds->s_start, spds->total_lastS, spds->fsmd1, points_to_write1);

			(spds->r_start)++;
			(spds->r_end)++;
		}
		while (spds->s_start < spds->lastS)
		{
			udFS_intermediate_discovery_set_internalLoop( spds->s_start, spds->s_end, spds->r_start, spds->total_lastR, spds->fsmd2, points_to_write2);

			(spds->s_start)++;
			(spds->s_end)++;
		}
	}

	return NULL;
}

void udFS_intermediate_discovery_set(Relation &R, Relation &S, FSMapDetached* fsmd_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2)
{
	StructForParallelDiscoverySet spds[numThreads];
	pthread_t thread_id[numThreads];

	for (uint32_t i = 0; i < numThreads; i++)
	{
		spds[i].r_start = boundariesR[i] + 1;
		spds[i].s_start = boundariesS[i] + 1;
		spds[i].r_end   = R.endpoints + (spds[i].r_start - R.startpoints);
		spds[i].s_end   = S.endpoints + (spds[i].s_start - S.startpoints);
		spds[i].lastR   = boundariesR[i+1] + 1;
		spds[i].lastS   = boundariesS[i+1] + 1;
		spds[i].total_lastR = boundariesR[numThreads] + 1;
		spds[i].total_lastS = boundariesS[numThreads] + 1;
		spds[i].fsmd1 = &fsmd_intermediate[0];
		spds[i].fsmd2 = &fsmd_intermediate[1];

		spds[i].thread_id = i;
		spds[i].points_to_write1 = &points_to_write1[i];
		spds[i].points_to_write2 = &points_to_write2[i];

		pthread_create( &thread_id[i], NULL, udFS_intermediate_discovery_set_thread, (void*) &spds[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}
}

void* bgudFS_intermediate_discovery_set_thread(void* args)
{
	StructForParallelDiscoverySet* spds = (StructForParallelDiscoverySet*) args;

	uint32_t points_to_write1 = *(spds->points_to_write1);
	uint32_t points_to_write2 = *(spds->points_to_write2);

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_end, **pointers_group_start;

	while ((spds->r_start < spds->lastR) && (spds->s_start < spds->lastS))
	{
		if (*(spds->r_start) < *(spds->s_start))
		{
			// Step 1: gather group for R.
			while ((spds->r_start < spds->lastR) && (*(spds->r_start) < *(spds->s_start)) && (group_size <= spds->max_group_size) )
			{
				group_size++;
				(spds->r_start)++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = (spds->r_end)++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = spds->R_startpoints + (pointers_group_end[i] - spds->R_endpoints);

			bgudFS_intermediate_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, spds->s_start, spds->total_lastS, *(spds->BIS), *(spds->S_startpoints), spds->fsmg1, points_to_write1);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
		else
		{
			// Step 1: gather group for S.
			while ((spds->s_start < spds->lastS) && (*(spds->r_start) >= *(spds->s_start)) && (group_size <= spds->max_group_size) )
			{
				group_size++;
				(spds->s_start)++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = (spds->s_end)++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = spds->S_startpoints + (pointers_group_end[i] - spds->S_endpoints);

			bgudFS_intermediate_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, spds->r_start, spds->total_lastR, *(spds->BIR), *(spds->R_startpoints), spds->fsmg2, points_to_write2);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
	}

	if ( (spds->lastS != spds->total_lastS) && (spds->lastR != spds->total_lastR) )
	{
		while (spds->r_start < spds->lastR)
		{
			// Step 1: gather group for R.
			while ((spds->r_start < spds->lastR) && (group_size <= spds->max_group_size) )
			{
				group_size++;
				(spds->r_start)++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = (spds->r_end)++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = spds->R_startpoints + (pointers_group_end[i] - spds->R_endpoints);

			bgudFS_intermediate_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, spds->s_start, spds->total_lastS, *(spds->BIS), *(spds->S_startpoints), spds->fsmg1, points_to_write1);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
		while (spds->s_start < spds->lastS)
		{
			// Step 1: gather group for S.
			while ((spds->s_start < spds->lastS) && (group_size <= spds->max_group_size) )
			{
				group_size++;
				(spds->s_start)++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = (spds->s_end)++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = spds->S_startpoints + (pointers_group_end[i] - spds->S_endpoints);

			bgudFS_intermediate_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, spds->r_start, spds->total_lastR, *(spds->BIR), *(spds->R_startpoints), spds->fsmg2, points_to_write2);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
	}

	return NULL;
}

void bgudFS_intermediate_discovery_set(Relation &R, Relation &S, int max_group_size, BucketIndex& BIR, BucketIndex& BIS, FSMapGrouped* fsmg_intermediate, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t numThreads, uint32_t* points_to_write1, uint32_t* points_to_write2)
{
	StructForParallelDiscoverySet spds[numThreads];
	pthread_t thread_id[numThreads];

	for (uint32_t i = 0; i < numThreads; i++)
	{
		spds[i].r_start = boundariesR[i] + 1;
		spds[i].s_start = boundariesS[i] + 1;
		spds[i].r_end   = R.endpoints + (spds[i].r_start - R.startpoints);
		spds[i].s_end   = S.endpoints + (spds[i].s_start - S.startpoints);
		spds[i].lastR   = boundariesR[i+1] + 1;
		spds[i].lastS   = boundariesS[i+1] + 1;
		spds[i].total_lastR = boundariesR[numThreads] + 1;
		spds[i].total_lastS = boundariesS[numThreads] + 1;
		spds[i].max_group_size = max_group_size;
		spds[i].BIR = &BIR;
		spds[i].BIS = &BIS;
		spds[i].R_startpoints = R.startpoints;
		spds[i].S_startpoints = S.startpoints;
		spds[i].R_endpoints   = R.endpoints;
		spds[i].S_endpoints   = S.endpoints;
		spds[i].fsmg1 = &fsmg_intermediate[0];
		spds[i].fsmg2 = &fsmg_intermediate[1];

		spds[i].thread_id = i;
		spds[i].points_to_write1 = &points_to_write1[i];
		spds[i].points_to_write2 = &points_to_write2[i];

		pthread_create( &thread_id[i], NULL, bgudFS_intermediate_discovery_set_thread, (void*) &spds[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}
}

////////////////////
// Internal loops //
////////////////////

void udFS_intermediate_discovery_set_internalLoop( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd_intermediate, uint32_t& points_to_write)
{
	Timestamp* pivot_start = firstFS;

	while ( ((pivot_start+32) < lastFS) && (*(pivot_start+31) < *curr_end) )
	{
		pivot_start += 32;
	}

	while ( (pivot_start != lastFS) && (*pivot_start < *curr_end) )
	{
		pivot_start++;
	}

	if (firstFS != pivot_start)
	{
		fsmd_intermediate->result_list[ points_to_write ].curr  = curr_start;
		fsmd_intermediate->result_list[ points_to_write ].start = firstFS;
		fsmd_intermediate->result_list[ points_to_write ].end   = pivot_start;
		points_to_write++;
	}
}

void bgudFS_intermediate_discovery_set_internalLoop  ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size,
Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg_intermediate, uint32_t& points_to_write)
{
	long int last_bucket;
	bool wrote = false;

	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot, *begin;
	Timestamp** lastG = pointers_group_end + group_size;

	for (Timestamp **curr_end = pointers_group_end, **curr_start = pointers_group_start; curr_end != lastG; curr_end++, curr_start++)
	{
		bufferSize = lastG-curr_end;

//		if (pivot == lastFS)
//			break;
		if ( *(*curr_end) < minStart)
			continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		// move pivot to the last bucket
		pivot = firstFS;
		if (last_bucket > 0)
			pivot = BI.bucket_list_decomposed[last_bucket-1];

		// Sweep the last bucket.
		last = BI.bucket_list_decomposed[last_bucket];
		while ((last-pivot >= 32) && (*(*curr_end) > *(pivot+31)))
		{
			pivot += 32;
		}

		while ((pivot < last) && (*pivot < *(*curr_end)))
		{
			pivot++;
		}

		if (pivot > firstFS)
		{
			if (!wrote)
			{
				wrote = true;
				fsmg_intermediate->result_list[ points_to_write ].pointers_group_start =
					(Timestamp**) malloc(bufferSize * sizeof(Timestamp*) );
				fsmg_intermediate->result_list[ points_to_write ].start = firstFS;
				fsmg_intermediate->result_list[ points_to_write ].end =
					(Timestamp**) malloc(bufferSize * sizeof(Timestamp*) );
				fsmg_intermediate->result_list[ points_to_write ].group_size = 0;
			}

			fsmg_intermediate->result_list[ points_to_write ].
				pointers_group_start[ fsmg_intermediate->result_list[ points_to_write ].group_size ] = *curr_start;
			fsmg_intermediate->result_list[ points_to_write ].
				end                 [ fsmg_intermediate->result_list[ points_to_write ].group_size ] = pivot;
			fsmg_intermediate->result_list[ points_to_write ].group_size++;
		}
	}

	if (wrote)
	{
		points_to_write++;
	}
}

////////////////////
////// Splits //////
////////////////////

void udFS_intermediate_discovery_split_count(FSMapDetached* fsmd_intermediate, FSMapDetached* fsmd, uint32_t numThreads)
{
	uint32_t current_load, smallest_index;

	for (uint32_t i = 0; i < fsmd_intermediate[0].numResults; i++)
	{
		current_load = fsmd_intermediate[0].result_list[i].end - fsmd_intermediate[0].result_list[i].start;

		smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

		fsmd[smallest_index].load += current_load;
		fsmd[smallest_index].numResults++;
	}

	for (uint32_t i = 0; i < fsmd_intermediate[1].numResults; i++)
	{
		current_load = fsmd_intermediate[1].result_list[i].end - fsmd_intermediate[1].result_list[i].start;

		smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

		fsmd[smallest_index].load += current_load;
		fsmd[smallest_index].numResults++;
	}

	for (uint32_t i = 0; i < numThreads; i++)
	{
		fsmd[i].result_list = (fsmd[i].numResults != 0) ? (FSDetached*) malloc( fsmd[i].numResults * sizeof(FSDetached) ) : NULL;
		fsmd[i].numResults = 0;
		fsmd[i].load = 0;
	}
}

void udFS_intermediate_discovery_split_set(FSMapDetached* fsmd_intermediate, FSMapDetached* fsmd, uint32_t numThreads, Relation& R, Relation& S, Relation* R_copies, Relation* S_copies, int allocationStrategy, int* thread_core_map)
{
	uint32_t current_load, smallest_index;
	int numa_map[numThreads];

	// numa functions take time, so use a numa_map to call them less frequently
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

	if (allocationStrategy != OS)
	{
		for (uint32_t i = 0; i < fsmd_intermediate[0].numResults; i++)
		{
			current_load = fsmd_intermediate[0].result_list[i].end - fsmd_intermediate[0].result_list[i].start;

			smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].curr  = R_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[0].result_list[i].curr  - R.startpoints);
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].start = S_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[0].result_list[i].start - S.startpoints);
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].end   = S_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[0].result_list[i].end   - S.startpoints);
			fsmd[smallest_index].load += current_load;
			fsmd[smallest_index].numResults++;
		}

		for (uint32_t i = 0; i < fsmd_intermediate[1].numResults; i++)
		{
			current_load = fsmd_intermediate[1].result_list[i].end - fsmd_intermediate[1].result_list[i].start;

			smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].curr  = S_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[1].result_list[i].curr  - S.startpoints);
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].start = R_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[1].result_list[i].start - R.startpoints);
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].end   = R_copies[ numa_map[smallest_index] ].startpoints + (fsmd_intermediate[1].result_list[i].end   - R.startpoints);
			fsmd[smallest_index].load += current_load;
			fsmd[smallest_index].numResults++;
		}
	}
	else
	{
		for (uint32_t i = 0; i < fsmd_intermediate[0].numResults; i++)
		{
			current_load = fsmd_intermediate[0].result_list[i].end - fsmd_intermediate[0].result_list[i].start;

			smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].curr  = fsmd_intermediate[0].result_list[i].curr;
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].start = fsmd_intermediate[0].result_list[i].start;
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].end   = fsmd_intermediate[0].result_list[i].end;
			fsmd[smallest_index].load += current_load;
			fsmd[smallest_index].numResults++;
		}

		for (uint32_t i = 0; i < fsmd_intermediate[1].numResults; i++)
		{
			current_load = fsmd_intermediate[1].result_list[i].end - fsmd_intermediate[1].result_list[i].start;

			smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].curr  = fsmd_intermediate[1].result_list[i].curr;
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].start = fsmd_intermediate[1].result_list[i].start;
			fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].end   = fsmd_intermediate[1].result_list[i].end;
			fsmd[smallest_index].load += current_load;
			fsmd[smallest_index].numResults++;
		}
	}
}

void bgudFS_intermediate_discovery_split_count(FSMapGrouped* fsmg_intermediate, FSMapGrouped* fsmg, uint32_t numThreads)
{
	uint32_t current_load, smallest_index;

	for (uint32_t i = 0; i < fsmg_intermediate[0].numResults; i++)
	{
		current_load = 0;
		for (uint32_t j = 0; j < fsmg_intermediate[0].result_list[i].group_size; j++)
		{
			current_load += fsmg_intermediate[0].result_list[i].end[j] - fsmg_intermediate[0].result_list[i].start;
		}

		smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

		fsmg[smallest_index].load += current_load;
		fsmg[smallest_index].numResults++;
	}

	for (uint32_t i = 0; i < fsmg_intermediate[1].numResults; i++)
	{
		current_load = 0;
		for (uint32_t j = 0; j < fsmg_intermediate[1].result_list[i].group_size; j++)
		{
			current_load += fsmg_intermediate[1].result_list[i].end[j] - fsmg_intermediate[1].result_list[i].start;
		}

		smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

		fsmg[smallest_index].load += current_load;
		fsmg[smallest_index].numResults++;
	}

	for (uint32_t i = 0; i < numThreads; i++)
	{
		fsmg[i].result_list = (fsmg[i].numResults != 0) ? (FSGrouped*) malloc( fsmg[i].numResults * sizeof(FSGrouped) ) : NULL;
		fsmg[i].numResults = 0;
		fsmg[i].load = 0;
	}
}

void bgudFS_intermediate_discovery_split_set(FSMapGrouped* fsmg_intermediate, FSMapGrouped* fsmg, uint32_t numThreads, Relation& R, Relation& S, Relation* R_copies, Relation* S_copies, int allocationStrategy, int* thread_core_map)
{
	uint32_t current_load, smallest_index;

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

	if (allocationStrategy != OS)
	{
		for (uint32_t i = 0; i < fsmg_intermediate[0].numResults; i++)
		{
			current_load = 0;
			for (uint32_t j = 0; j < fsmg_intermediate[0].result_list[i].group_size; j++)
			{
				current_load += fsmg_intermediate[0].result_list[i].end[j] - fsmg_intermediate[0].result_list[i].start;
			}

			smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start = (Timestamp**) malloc(fsmg_intermediate[0].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].start = S_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[0].result_list[i].start - S.startpoints);
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                  = (Timestamp**) malloc(fsmg_intermediate[0].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size = fsmg_intermediate[0].result_list[i].group_size;
			for (uint32_t j = 0; j < fsmg_intermediate[0].result_list[i].group_size; j++)
			{
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start[ j ] =  R_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[0].result_list[i].pointers_group_start[j] - R.startpoints);
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                 [ j ] =  S_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[0].result_list[i].end[j] - S.startpoints);
			}
			fsmg[smallest_index].load += current_load;
			fsmg[smallest_index].numResults++;
		}

		for (uint32_t i = 0; i < fsmg_intermediate[1].numResults; i++)
		{
			current_load = 0;
			for (uint32_t j = 0; j < fsmg_intermediate[1].result_list[i].group_size; j++)
			{
				current_load += fsmg_intermediate[1].result_list[i].end[j] - fsmg_intermediate[1].result_list[i].start;
			}

			smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start = (Timestamp**) malloc(fsmg_intermediate[1].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].start  = R_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[1].result_list[i].start - R.startpoints);
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                  = (Timestamp**) malloc(fsmg_intermediate[1].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size = fsmg_intermediate[1].result_list[i].group_size;
			for (uint32_t j = 0; j < fsmg_intermediate[1].result_list[i].group_size; j++)
			{
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start[ j ] = S_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[1].result_list[i].pointers_group_start[j] - S.startpoints);
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                 [ j ] = R_copies[ numa_map[smallest_index] ].startpoints + (fsmg_intermediate[1].result_list[i].end[j] - R.startpoints);
			}
			fsmg[smallest_index].load += current_load;
			fsmg[smallest_index].numResults++;
		}
	}
	else
	{
		for (uint32_t i = 0; i < fsmg_intermediate[0].numResults; i++)
		{
			current_load = 0;
			for (uint32_t j = 0; j < fsmg_intermediate[0].result_list[i].group_size; j++)
			{
				current_load += fsmg_intermediate[0].result_list[i].end[j] - fsmg_intermediate[0].result_list[i].start;
			}

			smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start = (Timestamp**) malloc(fsmg_intermediate[0].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].start = fsmg_intermediate[0].result_list[i].start;
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                  = (Timestamp**) malloc(fsmg_intermediate[0].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size = fsmg_intermediate[0].result_list[i].group_size;
			for (uint32_t j = 0; j < fsmg_intermediate[0].result_list[i].group_size; j++)
			{
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start[ j ] = fsmg_intermediate[0].result_list[i].pointers_group_start[j];
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                 [ j ] = fsmg_intermediate[0].result_list[i].end[j];
			}
			fsmg[smallest_index].load += current_load;
			fsmg[smallest_index].numResults++;
		}

		for (uint32_t i = 0; i < fsmg_intermediate[1].numResults; i++)
		{
			current_load = 0;
			for (uint32_t j = 0; j < fsmg_intermediate[1].result_list[i].group_size; j++)
			{
				current_load += fsmg_intermediate[1].result_list[i].end[j] - fsmg_intermediate[1].result_list[i].start;
			}

			smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start = (Timestamp**) malloc(fsmg_intermediate[1].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].start = fsmg_intermediate[1].result_list[i].start;
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                  = (Timestamp**) malloc(fsmg_intermediate[1].result_list[i].group_size * sizeof(Timestamp*) );
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size = fsmg_intermediate[1].result_list[i].group_size;
			for (uint32_t j = 0; j < fsmg_intermediate[1].result_list[i].group_size; j++)
			{
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start[ j ] = fsmg_intermediate[1].result_list[i].pointers_group_start[j];
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                 [ j ] = fsmg_intermediate[1].result_list[i].end[j];
			}
			fsmg[smallest_index].load += current_load;
			fsmg[smallest_index].numResults++;
		}
	}
}
