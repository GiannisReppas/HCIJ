#include "../def.hpp"
#include "../containers/relation.hpp"

// internal loops defined in different file
uint64_t udFS_internalLoop_wc          ( Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);
uint64_t udFS_internalLoop_xor         ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);
uint64_t udFS_internalLoop_simd_xor    ( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);

// unused rolled version at the end of file for easier debugging
uint64_t dFS_internalLoop( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS);

// main udFS algorithm for wc calculation
uint64_t udFS_wc(Relation &R, Relation &S)
{
	uint64_t result = 0;

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
			result += udFS_internalLoop_wc( r_end, s_start, lastS);

			r_start++;
			r_end++;
		}
		else
		{
			result += udFS_internalLoop_wc( s_end, r_start, lastR);

			s_start++;
			s_end++;
		}
	}

	return result;
}

// main udFS algorithm for regular match-xor calculation
uint64_t udFS_xor(Relation &R, Relation &S)
{
	uint64_t result = 0;

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
			result += udFS_internalLoop_xor( r_start, r_end, s_start, lastS);

			r_start++;
			r_end++;
		}
		else
		{
			result += udFS_internalLoop_xor( s_start, s_end, r_start, lastR);

			s_start++;
			s_end++;
		}
	}

	return result;
}

// main udFS algorithm for match-xor calculation using SIMD
uint64_t udFS_simd_xor(Relation &R, Relation &S)
{
	uint64_t result = 0;

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
			result += udFS_internalLoop_simd_xor( r_start, r_end, s_start, lastS);

			r_start++;
			r_end++;
		}
		else
		{
			result += udFS_internalLoop_simd_xor( s_start, s_end, r_start, lastR);

			s_start++;
			s_end++;
		}
	}

	return result;
}

////////////////////
// Internal loops //
//////////////////// // keeping rolled version for easier debugging

// rolled
uint64_t dFS_internalLoop( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS)
{
	uint64_t result = 0;

	Timestamp* pivot_start = firstFS;

	while ( (pivot_start != lastFS) && (*pivot_start < *curr_end) )
	{
#if defined(WORKLOAD_COUNT)
		result++;
#elif defined(XOR)
		result += *curr_start ^ *pivot;
#elif defined(SIMD_XOR)
		printf("simd can only be applied on an unrolled version\n");
		exit(1);
#endif

		pivot_start++;
	}

	return result;
}

uint64_t udFS_internalLoop_wc( Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS)
{
	uint64_t result = 0;

	Timestamp* pivot_start = firstFS;

	while ( ((pivot_start+32) < lastFS) && (*(pivot_start+31) < *curr_end) )
	{
		result += 32;

		pivot_start += 32;
	}

	while ( (pivot_start != lastFS) && (*pivot_start < *curr_end) )
	{
		result += 1;

		pivot_start++;
	}

	return result;
}

uint64_t udFS_internalLoop_xor( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS)
{
	uint64_t result = 0;

	Timestamp* pivot_start = firstFS;

	while ( ((pivot_start+32) < lastFS) && (*(pivot_start+31) < *curr_end) )
	{
		result += *curr_start ^ *(pivot_start+0);
		result += *curr_start ^ *(pivot_start+1);
		result += *curr_start ^ *(pivot_start+2);
		result += *curr_start ^ *(pivot_start+3);
		result += *curr_start ^ *(pivot_start+4);
		result += *curr_start ^ *(pivot_start+5);
		result += *curr_start ^ *(pivot_start+6);
		result += *curr_start ^ *(pivot_start+7);
		result += *curr_start ^ *(pivot_start+8);
		result += *curr_start ^ *(pivot_start+9);
		result += *curr_start ^ *(pivot_start+10);
		result += *curr_start ^ *(pivot_start+11);
		result += *curr_start ^ *(pivot_start+12);
		result += *curr_start ^ *(pivot_start+13);
		result += *curr_start ^ *(pivot_start+14);
		result += *curr_start ^ *(pivot_start+15);
		result += *curr_start ^ *(pivot_start+16);
		result += *curr_start ^ *(pivot_start+17);
		result += *curr_start ^ *(pivot_start+18);
		result += *curr_start ^ *(pivot_start+19);
		result += *curr_start ^ *(pivot_start+20);
		result += *curr_start ^ *(pivot_start+21);
		result += *curr_start ^ *(pivot_start+22);
		result += *curr_start ^ *(pivot_start+23);
		result += *curr_start ^ *(pivot_start+24);
		result += *curr_start ^ *(pivot_start+25);
		result += *curr_start ^ *(pivot_start+26);
		result += *curr_start ^ *(pivot_start+27);
		result += *curr_start ^ *(pivot_start+28);
		result += *curr_start ^ *(pivot_start+29);
		result += *curr_start ^ *(pivot_start+30);
		result += *curr_start ^ *(pivot_start+31);

		pivot_start += 32;
	}

	while ( (pivot_start != lastFS) && (*pivot_start < *curr_end) )
	{
		result += *curr_start ^ *pivot_start;

		pivot_start++;
	}

	return result;
}

uint64_t udFS_internalLoop_simd_xor( Timestamp* curr_start, Timestamp* curr_end, Timestamp* firstFS, Timestamp* lastFS)
{
	uint64_t result = 0;
	__m256i vecA = _mm256_set1_epi64x(*curr_start), vecResult = _mm256_set1_epi64x(0), vecB;
/*
	Timestamp helper_list[4];
	helper_list[0] = *curr_start;
	helper_list[1] = *curr_start;
	helper_list[2] = *curr_start;
	helper_list[3] = *curr_start;
	vecA      = _mm256_loadu_si256(reinterpret_cast<__m256i*>(helper_list));
	uint64_t result_list[4] = {0,0,0,0};
	vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
*/
	Timestamp* pivot_start = firstFS;
	while ( ((pivot_start+32) < lastFS) && (*(pivot_start+31) < *curr_end) )
	{
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+0));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+4));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+8));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+12));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+16));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+20));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+24));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot_start+28));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);

		pivot_start += 32;
	}

	uint64_t result_list[4] = {0,0,0,0};
	_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

	while ( (pivot_start != lastFS) && (*pivot_start < *curr_end) )
	{
		result += *curr_start ^ *pivot_start;

		pivot_start++;
	}

	result += result_list[0] + result_list[1] + result_list[2] + result_list[3];

	return result;
}
