#include "../def.hpp"
#include "../containers/relation.hpp"
#include "../containers/bucket_index.hpp"

// internal loops defined in different file
uint64_t bgudFS_internalLoop_wc		  ( Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_xor		 ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_simd_xor	( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);

// unused rolled version at the end of file for easier debugging
uint64_t bgdFS_internalLoop( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);

// sorting condition for grouping optimization
static bool CompareByEndPointersT( Timestamp*& a, Timestamp*& b);

#define xor_unroll(result, val, pivot) \
	result += (val) ^ *(pivot+0); \
	result += (val) ^ *(pivot+1); \
	result += (val) ^ *(pivot+2); \
	result += (val) ^ *(pivot+3); \
	result += (val) ^ *(pivot+4); \
	result += (val) ^ *(pivot+5); \
	result += (val) ^ *(pivot+6); \
	result += (val) ^ *(pivot+7); \
	result += (val) ^ *(pivot+8); \
	result += (val) ^ *(pivot+9); \
	result += (val) ^ *(pivot+10); \
	result += (val) ^ *(pivot+11); \
	result += (val) ^ *(pivot+12); \
	result += (val) ^ *(pivot+13); \
	result += (val) ^ *(pivot+14); \
	result += (val) ^ *(pivot+15); \
	result += (val) ^ *(pivot+16); \
	result += (val) ^ *(pivot+17); \
	result += (val) ^ *(pivot+18); \
	result += (val) ^ *(pivot+19); \
	result += (val) ^ *(pivot+20); \
	result += (val) ^ *(pivot+21); \
	result += (val) ^ *(pivot+22); \
	result += (val) ^ *(pivot+23); \
	result += (val) ^ *(pivot+24); \
	result += (val) ^ *(pivot+25); \
	result += (val) ^ *(pivot+26); \
	result += (val) ^ *(pivot+27); \
	result += (val) ^ *(pivot+28); \
	result += (val) ^ *(pivot+29); \
	result += (val) ^ *(pivot+30); \
	result += (val) ^ *(pivot+31);

#define simd_xor_unroll(vecResult, vecB, pivot, vecA, group_index, step_group_index) \
	vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(pivot+0)); \
	vecB = _mm512_xor_si512(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm512_add_epi64(vecResult,vecB); \
	vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(pivot+8)); \
	vecB = _mm512_xor_si512(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm512_add_epi64(vecResult,vecB); \
	vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(pivot+16)); \
	vecB = _mm512_xor_si512(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm512_add_epi64(vecResult,vecB); \
	vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(pivot+24)); \
	vecB = _mm512_xor_si512(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm512_add_epi64(vecResult,vecB);
/*
#define simd_xor_unroll(vecResult, vecB, pivot, vecA, group_index, step_group_index) \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+0)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+4)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+8)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+12)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+16)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+20)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+24)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB); \
	vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+28)); \
	vecB = _mm256_xor_si256(vecB, vecA[group_index+step_group_index]); \
	vecResult = _mm256_add_epi64(vecResult,vecB);
*/

// main bgudFS algorithm for wc calculation
uint64_t bgudFS_wc(Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS)
{
	uint64_t result = 0;

	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_end, **pointers_group_start;

//uint64_t all = 0;
//uint64_t count = 0;

	while ((r_start < lastR) && (s_start < lastS) )
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

			// Step 2: run internal loop.
//std::cout << group_size << std::endl;
//all += group_size;
//count++;
			result += bgudFS_internalLoop_wc( pointers_group_end, group_size, s_start, lastS, BIS, S.minStart);

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

			// Step 2: run internal loop.
//std::cout << group_size << std::endl;
//all += group_size;
//count++;
			result += bgudFS_internalLoop_wc( pointers_group_end, group_size, r_start, lastR, BIR, R.minStart);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);

			group_size = 0;
		}
	}

//std::cout << (float) all / count << std::endl;

	return result;
}

// main bgudFS algorithm for regular match-xor calculation
uint64_t bgudFS_xor(Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS)
{
	uint64_t result = 0;

	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_end, ** pointers_group_start;

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

			// Step 2: run internal loop.
			result += bgudFS_internalLoop_xor( pointers_group_start, pointers_group_end, group_size, s_start, lastS, BIS, S.minStart);

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

			// Step 2: run internal loop.
			result += bgudFS_internalLoop_xor( pointers_group_start , pointers_group_end , group_size, r_start, lastR, BIR, R.minStart);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
	}

	return result;
}

// main bgudFS algorithm, for match-xor calculation using SIMD
uint64_t bgudFS_simd_xor(Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS)
{
	uint64_t result = 0;

	Timestamp* r_start = R.startpoints;
	Timestamp* s_start = S.startpoints;
	Timestamp* r_end   = R.endpoints;
	Timestamp* s_end   = S.endpoints;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t i, group_size = 0;
	Timestamp **pointers_group_start, **pointers_group_end;

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

			// Step 2: run internal loop.
			result += bgudFS_internalLoop_simd_xor( pointers_group_start, pointers_group_end, group_size, s_start, lastS, BIS, S.minStart);

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

			// Step 2: run internal loop.
			result += bgudFS_internalLoop_simd_xor( pointers_group_start, pointers_group_end, group_size, r_start, lastR, BIR, R.minStart);

			// Step 3: empty current group.
			free(pointers_group_start);
			free(pointers_group_end);
			group_size = 0;
		}
	}

	return result;
}

////////////////////
// Internal loops //
//////////////////// // keeping rolled version for easier debugging

// rolled
uint64_t bgdFS_internalLoop( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart)
{
	uint64_t result = 0;

	long int last_bucket;

	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot_start = firstFS;
	Timestamp **lastG_end   = pointers_group_end + group_size;

	for (Timestamp **curr_end = pointers_group_end, **curr_start = pointers_group_start; curr_end != lastG_end; curr_end++, curr_start++)
	{
		bufferSize = lastG_end - curr_end;

		if (pivot_start == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		// Sweep previous buckets
		if (last_bucket > 0)
		{
			last = BI.bucket_list_decomposed[last_bucket-1];
			while (pivot_start < last)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
#if defined(WORKLOAD_COUNT)
					result++;
#elif defined(XOR)
					result += *(*(curr_start+i)) ^ *(pivot_start+0);
#elif defined(SIMD_XOR)
					printf("simd can only be applied on an unrolled version\n");
					exit(1);
#endif
				}

				pivot_start++;
			}
		}

		// Sweep the last bucket
		Timestamp* last = BI.bucket_list_decomposed[last_bucket];
		while ((pivot_start != last) && ( *(*curr_end) > *pivot_start))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
#if defined(WORKLOAD_COUNT)
				result++;
#elif defined(XOR)
				result += *(*(curr_start+i)) ^ *(pivot_start+0);
#elif defined(SIMD_XOR)
				printf("simd can only be applied on an unrolled version\n");
				exit(1);
#endif
			}

			pivot_start++;
		}
	}

	return result;
}

uint64_t bgudFS_internalLoop_wc( Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart)
{
	uint64_t result = 0;

	long int last_bucket;

	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot_start = firstFS;
	Timestamp **lastG_end = pointers_group_end + group_size;

	for (Timestamp** curr_end = pointers_group_end; curr_end != lastG_end; curr_end++)
	{
		bufferSize = lastG_end - curr_end;

		if (pivot_start == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		// Sweep previous buckets
		if (last_bucket > 0)
		{
			last = BI.bucket_list_decomposed[last_bucket-1];
			while (last-pivot_start >= 32)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					result += 32;
				}

				pivot_start += 32;
			}

			while (pivot_start < last)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					result += 1;
				}

				pivot_start++;
			}
		}

		// Sweep the last bucket
		last = BI.bucket_list_decomposed[last_bucket];
		while ((last-pivot_start >= 32) && (*(*curr_end) > *(pivot_start+31)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += 32;
			}

			pivot_start += 32;
		}

		while ((pivot_start < last) && (*pivot_start < *(*curr_end)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += 1;
			}

			pivot_start++;
		}
	}

	return result;
}

//__attribute__((optimize("no-tree-vectorize")))
uint64_t bgudFS_internalLoop_xor( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart)
{
	uint64_t result = 0;
	long int last_bucket;
	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot_start = firstFS;
	Timestamp **lastG_end = pointers_group_end + group_size;

	Timestamp local_starts[group_size];
	for (uint32_t i = 0; i < group_size; i++)
		local_starts[i] = *(pointers_group_start[i]);
	
	Timestamp *local_curr_start = local_starts;

	for (Timestamp **curr_end = pointers_group_end; curr_end != lastG_end; curr_end++, local_curr_start++)
	{
		bufferSize = lastG_end - curr_end;

		if (pivot_start == lastFS) break;
		if ( *(*curr_end) < minStart) continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets) last_bucket = BI.numBuckets-1;

		if (last_bucket > 0)
		{
			last = BI.bucket_list_decomposed[last_bucket-1];
			while (last-pivot_start >= 32)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					xor_unroll(result, *(local_curr_start+i), pivot_start);
				}
				pivot_start += 32;
			}

			while (pivot_start < last)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					result += *(local_curr_start+i) ^ *(pivot_start+0);
				}
				pivot_start++;
			}
		}

		last = BI.bucket_list_decomposed[last_bucket];
		while ((last-pivot_start >= 32) && (*(*curr_end) > *(pivot_start+31)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				xor_unroll(result, *(local_curr_start+i), pivot_start);
			}
			pivot_start += 32;
		}

		while ((pivot_start < last) && (*pivot_start < *(*curr_end)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += *(local_curr_start+i) ^ *(pivot_start+0);
			}
			pivot_start++;
		}
	}

	return result;
}

/*
uint64_t bgudFS_internalLoop_simd_xor( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart)
{
	uint64_t result = 0;

	long int last_bucket;

	__m256i *vecA = (__m256i*) aligned_alloc( 32, sizeof(__m256i) * group_size), vecB, vecResult;
	Timestamp **helper_list = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
	for (uint32_t i = 0; i < group_size; i++)
	{
		helper_list[i] = (Timestamp*) malloc( 4*sizeof(Timestamp) );
		helper_list[i][0] = *(*(pointers_group_start+i));
		helper_list[i][1] = *(*(pointers_group_start+i));
		helper_list[i][2] = *(*(pointers_group_start+i));
		helper_list[i][3] = *(*(pointers_group_start+i));
		vecA[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(helper_list[i]));
	}
	uint64_t result_list[4] = {0,0,0,0};
	vecResult = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(result_list));

	uint32_t group_index = 0;
	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot_start = firstFS;
	Timestamp **lastG_end = pointers_group_end + group_size;

	for (Timestamp **curr_end = pointers_group_end, **curr_start = pointers_group_start; curr_end != lastG_end; curr_end++, curr_start++, group_index++)
	{
		bufferSize = lastG_end - curr_end;

		if (pivot_start == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		// Sweep previous buckets
		if (last_bucket > 0)
		{
			last = BI.bucket_list_decomposed[last_bucket-1];
			while (last-pivot_start >= 32)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					simd_xor_unroll(vecResult, vecB, pivot_start, vecA, group_index, i);
				}

				pivot_start += 32;
			}

			while (pivot_start < last)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					result += *(*(curr_start+i)) ^ *(pivot_start+0);
				}

				pivot_start++;
			}
		}

		// Sweep the last bucket
		last = BI.bucket_list_decomposed[last_bucket];
		while ((last-pivot_start >= 32) && (*(*curr_end) > *(pivot_start+31)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				simd_xor_unroll(vecResult, vecB, pivot_start, vecA, group_index, i);
			}

			pivot_start += 32;
		}

		while ((pivot_start < last) && (*pivot_start < *(*curr_end)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += *(*(curr_start+i)) ^ *(pivot_start+0);
			}

			pivot_start++;
		}
	}

	_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);
	result += result_list[0] + result_list[1] + result_list[2] + result_list[3];

	free(vecA);
	for (uint32_t i = 0; i < group_size; i++)
		free( helper_list[i] );
	free(helper_list);

	return result;
}
*/

uint64_t bgudFS_internalLoop_simd_xor( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart)
{
	uint64_t result = 0;

	long int last_bucket;

	__m512i *vecA = (__m512i*) aligned_alloc( 64, sizeof(__m512i) * group_size), vecB, vecResult;
	Timestamp **helper_list = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
	for (uint32_t i = 0; i < group_size; i++)
	{
		helper_list[i] = (Timestamp*) malloc( 8*sizeof(Timestamp) );
		helper_list[i][0] = *(*(pointers_group_start+i));
		helper_list[i][1] = *(*(pointers_group_start+i));
		helper_list[i][2] = *(*(pointers_group_start+i));
		helper_list[i][3] = *(*(pointers_group_start+i));
		helper_list[i][4] = *(*(pointers_group_start+i));
		helper_list[i][5] = *(*(pointers_group_start+i));
		helper_list[i][6] = *(*(pointers_group_start+i));
		helper_list[i][7] = *(*(pointers_group_start+i));
		vecA[i] = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(helper_list[i]));
	}
	uint64_t result_list[8] = {0,0,0,0,0,0,0,0};
	vecResult = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(result_list));

	uint32_t group_index = 0;
	uint32_t bufferSize;
	Timestamp *last;
	Timestamp *pivot_start = firstFS;
	Timestamp **lastG_end = pointers_group_end + group_size;

	for (Timestamp **curr_end = pointers_group_end, **curr_start = pointers_group_start; curr_end != lastG_end; curr_end++, curr_start++, group_index++)
	{
		bufferSize = lastG_end - curr_end;

		if (pivot_start == lastFS)
			break;
		if ( *(*curr_end) < minStart)
			continue;

		last_bucket = ceil((double)( *(*curr_end) - minStart)/BI.bucket_range);
		if (last_bucket >= BI.numBuckets)
			last_bucket = BI.numBuckets-1;

		// Sweep previous buckets
		if (last_bucket > 0)
		{
			last = BI.bucket_list_decomposed[last_bucket-1];
			while (last-pivot_start >= 32)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					simd_xor_unroll(vecResult, vecB, pivot_start, vecA, group_index, i);
				}

				pivot_start += 32;
			}

			while (pivot_start < last)
			{
				for (uint32_t i = 0; i < bufferSize; i++)
				{
					result += *(*(curr_start+i)) ^ *(pivot_start+0);
				}

				pivot_start++;
			}
		}

		// Sweep the last bucket
		last = BI.bucket_list_decomposed[last_bucket];
		while ((last-pivot_start >= 32) && (*(*curr_end) > *(pivot_start+31)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				simd_xor_unroll(vecResult, vecB, pivot_start, vecA, group_index, i);
			}

			pivot_start += 32;
		}

		while ((pivot_start < last) && (*pivot_start < *(*curr_end)))
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += *(*(curr_start+i)) ^ *(pivot_start+0);
			}

			pivot_start++;
		}
	}

	_mm512_storeu_si512(reinterpret_cast<__m512i*>(result_list), vecResult);
	result += result_list[0] + result_list[1] + result_list[2] + result_list[3] + result_list[4] + result_list[5] + result_list[6] + result_list[7];

	free(vecA);
	for (uint32_t i = 0; i < group_size; i++)
	free( helper_list[i] );
	free(helper_list);

	return result;
}
