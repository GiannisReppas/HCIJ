#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"
#include "../../containers/FS_map.hpp"

// internal loops defined at the end of file
void   udFS_direct_discovery_count_internalLoop( Timestamp* curr_end,                        Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd, uint32_t numThreads);
void   udFS_direct_discovery_set_internalLoop  ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd, uint32_t numThreads);
void bgudFS_direct_discovery_count_internalLoop( Timestamp** pointers_group_end,                                   uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg, uint32_t numThreads);
void bgudFS_direct_discovery_set_internalLoop  ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg, uint32_t numThreads);

// sorting condition for grouping optimization
static bool CompareByEndPointersT( Timestamp*& a, Timestamp*& b);

// main udFS algorithms for direct result discovery calculation
void udFS_direct_discovery_count(Relation &R, Relation &S, FSMapDetached* fsmd, uint32_t numThreads)
{
	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	while ((r_start < lastR) && (s_start < lastS))
	{
		if (*r_start < *s_start)
		{
			udFS_direct_discovery_count_internalLoop( r_end, s_start, lastS, fsmd, numThreads);

			r_start++;
			r_end++;
		}
		else
		{
			udFS_direct_discovery_count_internalLoop( s_end, r_start, lastR, fsmd, numThreads);

			s_start++;
			s_end++;
		}
	}

	for (uint32_t i = 0; i < numThreads; i++)
	{
		fsmd[i].result_list = (fsmd[i].numResults != 0) ? (FSDetached*) malloc( fsmd[i].numResults * sizeof(FSDetached) ) : NULL;
		fsmd[i].numResults = 0;
		fsmd[i].load = 0;
	}
}

void udFS_direct_discovery_set(Relation &R, Relation &S, FSMapDetached* fsmd, uint32_t numThreads)
{
	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	while ((r_start < lastR) && (s_start < lastS))
	{
		if (*r_start < *s_start)
		{
			udFS_direct_discovery_set_internalLoop( r_start, r_end, s_start, lastS, fsmd, numThreads);

			r_start++;
			r_end++;
		}
		else
		{
			udFS_direct_discovery_set_internalLoop( s_start, s_end, r_start, lastR, fsmd, numThreads);

			s_start++;
			s_end++;
		}
	}
}

// main bgudFS algorithms for direct result discovery calculation
void bgudFS_direct_discovery_count(Relation &R, Relation &S, int max_group_size, BucketIndex &BIR, BucketIndex &BIS, FSMapGrouped* fsmg, uint32_t numThreads)
{
	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_end;

	while ((r_start < lastR) && (s_start < lastS))
	{
		if (*r_start < *s_start)
		{
			// Step 1: gather group for R.
			while ((r_start < lastR) && (*r_start < *s_start) && (group_size <= max_group_size) )
			{
				group_size++;
				r_start++;
			}

			// create pointers group and sort by end point.
			pointers_group_end = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = r_end++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);

			bgudFS_direct_discovery_count_internalLoop( pointers_group_end, group_size, s_start, lastS, BIS, S.minStart, fsmg, numThreads);

			// Step 3: empty current group.
			free(pointers_group_end);
			group_size = 0;
		}
		else
		{
			// Step 1: gather group for S.
			while ((s_start < lastS) && (*r_start >= *s_start) && (group_size <= max_group_size) )
			{
				group_size++;
				s_start++;
			}

			// create pointers group and sort by end point.
			pointers_group_end = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = s_end++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);

			bgudFS_direct_discovery_count_internalLoop( pointers_group_end, group_size, r_start, lastR, BIR, R.minStart, fsmg, numThreads);

			// Step 3: empty current group.
			free(pointers_group_end);
			group_size = 0;
		}
	}

	for (uint32_t i = 0; i < numThreads; i++)
	{
		fsmg[i].result_list = (fsmg[i].numResults != 0) ? (FSGrouped*) malloc( fsmg[i].numResults * sizeof(FSGrouped) ) : NULL;
		fsmg[i].numResults = 0;
		fsmg[i].load = 0;
	}
}


// main bgudFS algorithm for direct result discovery calculation
void bgudFS_direct_discovery_set(Relation &R, Relation &S, int max_group_size, BucketIndex &BIR, BucketIndex &BIS, FSMapGrouped* fsmg, uint32_t numThreads)
{
	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_end, **pointers_group_start;

	while ((r_start < lastR) && (s_start < lastS))
	{
		if (*r_start < *s_start)
		{
			// Step 1: gather group for R.
			while ((r_start < lastR) && (*r_start < *s_start) && (group_size <= max_group_size) )
			{
				group_size++;
				r_start++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = r_end++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = R.startpoints + (pointers_group_end[i] - R.endpoints);

			bgudFS_direct_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, s_start, lastS, BIS, S.minStart, fsmg, numThreads);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
		else
		{
			// Step 1: gather group for S.
			while ((s_start < lastS) && (*r_start >= *s_start) && (group_size <= max_group_size) )
			{
				group_size++;
				s_start++;
			}

			// create pointers group and sort by end point.
			pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
			for (i = 0; i < group_size; i++)
				pointers_group_end[i] = s_end++;
			std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
			for (i = 0; i < group_size; i++)
				pointers_group_start[i] = S.startpoints + (pointers_group_end[i] - S.endpoints);

			bgudFS_direct_discovery_set_internalLoop( pointers_group_start, pointers_group_end, group_size, r_start, lastR, BIR, R.minStart, fsmg, numThreads);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
	}
}

////////////////////
// Internal loops //
////////////////////

void udFS_direct_discovery_count_internalLoop( Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd, uint32_t numThreads)
{
	Timestamp* pivot = firstFS;

	while ( ((pivot+32) < lastFS) && (*(pivot+31) < *curr_end) )
	{
		pivot += 32;
	}

	while ( (pivot != lastFS) && (*pivot < *curr_end) )
	{
		pivot++;
	}

	uint64_t local_load = (pivot - firstFS);
	if (local_load)
	{
		uint32_t smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

		fsmd[smallest_index].load += local_load;
		fsmd[smallest_index].numResults++;
	}
}

void udFS_direct_discovery_set_internalLoop( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS, FSMapDetached* fsmd, uint32_t numThreads)
{
	Timestamp* pivot = firstFS;

	while ( ((pivot+32) < lastFS) && (*(pivot+31) < *curr_end) )
	{
		pivot += 32;
	}

	while ( (pivot != lastFS) && (*pivot < *curr_end) )
	{
		pivot++;
	}

	uint64_t local_load = (pivot - firstFS);
	if (local_load)
	{
		uint32_t smallest_index = std::distance( fsmd, std::min_element( fsmd, fsmd + numThreads));

		fsmd[smallest_index].load += local_load;
		fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].curr  = curr_start;
		fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].start = firstFS;
		fsmd[smallest_index].result_list[ fsmd[smallest_index].numResults ].end   = pivot;
		fsmd[smallest_index].numResults++;
	}
}

void bgudFS_direct_discovery_count_internalLoop( Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg, uint32_t numThreads)
{
	long int last_bucket;
	uint64_t local_load = 0;

	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot;
	Timestamp** lastG = pointers_group_end + group_size;
	for (Timestamp **curr_end = pointers_group_end; curr_end != lastG; curr_end++)
	{
		bufferSize = lastG-curr_end;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		if (last_bucket > 0)
			pivot = BI.bucket_list_decomposed[last_bucket-1];
		else
			pivot = firstFS;

		if (pivot == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

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

		local_load += (uint32_t) (pivot-firstFS);
	}

	if (local_load)
	{
		uint32_t smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));

		fsmg[smallest_index].load += local_load;
		fsmg[smallest_index].numResults++;
	}
}

void bgudFS_direct_discovery_set_internalLoop( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, BucketIndex& BI, Timestamp minStart, FSMapGrouped* fsmg, uint32_t numThreads)
{
	long int last_bucket;
	uint64_t local_load = 0;
	uint32_t smallest_index;
	bool wrote = false;

	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot;
	Timestamp** lastG = pointers_group_end + group_size;
	for (Timestamp **curr_end = pointers_group_end, **curr_start = pointers_group_start; curr_end != lastG; curr_end++, curr_start++)
	{
		bufferSize = lastG-curr_end;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		if (last_bucket > 0)
			pivot = BI.bucket_list_decomposed[last_bucket-1];
		else
			pivot = firstFS;

		if (pivot == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

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

		local_load += (uint64_t) (pivot-firstFS);

		if (pivot != firstFS)
		{
			if (!wrote)
			{
				smallest_index = std::distance( fsmg, std::min_element( fsmg, fsmg + numThreads));
				wrote = true;

				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start = (Timestamp**) malloc(bufferSize * sizeof(Timestamp*) );
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].start = firstFS;
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                  = (Timestamp**) malloc(bufferSize * sizeof(Timestamp*) );
				fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size = 0;
			}

			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].pointers_group_start[ fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size ] = *curr_start;
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].end                 [ fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size ] = pivot;
			fsmg[smallest_index].result_list[ fsmg[smallest_index].numResults ].group_size++;
		}
	}

	if (wrote)
	{
		fsmg[smallest_index].load += local_load;
		fsmg[smallest_index].numResults++;
	}
}
