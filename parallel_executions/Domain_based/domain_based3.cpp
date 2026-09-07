#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"

uint32_t create_partitions3( Relation&, Relation&, Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS,
							BucketIndex *psBIR, BucketIndex *psBIS,
							Timestamp domain_start, Timestamp domain_end, uint32_t numGranules, uint64_t granuleExtent, int numThreads, int allocationStrategy, int* thread_core_map);

uint64_t udFS_wc	   (Relation &R, Relation &S);
uint64_t udFS_xor	  (Relation &R, Relation &S);
uint64_t udFS_simd_xor (Relation &R, Relation &S);

uint64_t bgudFS_wc	   (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_xor	  (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_simd_xor (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);

uint64_t bgudFS_internalLoop_wc	   ( Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_xor	  ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_simd_xor ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);

int stick_this_thread_to_core(int core_id);

struct MiniJoinsStructureAtomic
{
	Relation *sR;
	Relation *sS;
	Relation *eR;
	Relation *eS;
	Relation *cR;
	Relation *cS;
	int max_group_size;
	BucketIndex *sBIR;
	BucketIndex *sBIS;

	int allocationStrategy;
	int FS_est;
	int mode;
	uint32_t thread_id;
	uint64_t core_id;
	uint64_t *result;
};

struct MiniJoinsStructureGreedy3
{
	Relation *psR;
	Relation *psS;
	Relation *peR;
	Relation *peS;
	Relation *pcR;
	Relation *pcS;
	int max_group_size;
	BucketIndex *psBIR;
	BucketIndex *psBIS;

	int allocationStrategy;
	int FS_est;
	int mode;
	std::vector< std::pair<uint32_t, uint32_t> >* mjoins;
	uint32_t thread_id;
	uint64_t core_id;
	uint64_t *result;
};

// NestedLoop to print all results without comparissons, used for (S,C) and (C,S) MiniJoins
uint64_t NestedLoop_wc(Relation &R, Relation &S)
{
	uint64_t result = 0;
	Timestamp *r, *s;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	for (r = R.startpoints; r != lastR; r++)
	{
		for (s = S.startpoints; (s+32) <= lastS; s += 32)
		{
			result += 32;
		}

		for (; s != lastS; s++)
		{
			result += 1;
		}
	}

	return result;
}

//__attribute__((optimize("no-tree-vectorize")))
uint64_t NestedLoop_xor(Relation &R, Relation &S)
{
	// following sota nested loop implementation
	uint64_t result = 0;
	Timestamp *lastR = R.startpoints + R.numRecords;
	Timestamp *lastS = S.startpoints + S.numRecords;

	Timestamp* s = S.startpoints;
	while (lastS-s >= 32)
	{
		for (Timestamp *k = R.startpoints; k != lastR; k++)
		{
			result += *k ^ *(s+0);
			result += *k ^ *(s+1);
			result += *k ^ *(s+2);
			result += *k ^ *(s+3);
			result += *k ^ *(s+4);
			result += *k ^ *(s+5);
			result += *k ^ *(s+6);
			result += *k ^ *(s+7);
			result += *k ^ *(s+8);
			result += *k ^ *(s+9);
			result += *k ^ *(s+10);
			result += *k ^ *(s+11);
			result += *k ^ *(s+12);
			result += *k ^ *(s+13);
			result += *k ^ *(s+14);
			result += *k ^ *(s+15);
			result += *k ^ *(s+16);
			result += *k ^ *(s+17);
			result += *k ^ *(s+18);
			result += *k ^ *(s+19);
			result += *k ^ *(s+20);
			result += *k ^ *(s+21);
			result += *k ^ *(s+22);
			result += *k ^ *(s+23);
			result += *k ^ *(s+24);
			result += *k ^ *(s+25);
			result += *k ^ *(s+26);
			result += *k ^ *(s+27);
			result += *k ^ *(s+28);
			result += *k ^ *(s+29);
			result += *k ^ *(s+30);
			result += *k ^ *(s+31);
		}

		s += 32;
	}

	while (s < lastS)
	{
		Timestamp *k = R.startpoints;

		while (lastR-k >= 32)
		{
			result += *(k+0) ^ *s;
			result += *(k+1) ^ *s;
			result += *(k+2) ^ *s;
			result += *(k+3) ^ *s;
			result += *(k+4) ^ *s;
			result += *(k+5) ^ *s;
			result += *(k+6) ^ *s;
			result += *(k+7) ^ *s;
			result += *(k+8) ^ *s;
			result += *(k+9) ^ *s;
			result += *(k+10) ^ *s;
			result += *(k+11) ^ *s;
			result += *(k+12) ^ *s;
			result += *(k+13) ^ *s;
			result += *(k+14) ^ *s;
			result += *(k+15) ^ *s;
			result += *(k+16) ^ *s;
			result += *(k+17) ^ *s;
			result += *(k+18) ^ *s;
			result += *(k+19) ^ *s;
			result += *(k+20) ^ *s;
			result += *(k+21) ^ *s;
			result += *(k+22) ^ *s;
			result += *(k+23) ^ *s;
			result += *(k+24) ^ *s;
			result += *(k+25) ^ *s;
			result += *(k+26) ^ *s;
			result += *(k+27) ^ *s;
			result += *(k+28) ^ *s;
			result += *(k+29) ^ *s;
			result += *(k+30) ^ *s;
			result += *(k+31) ^ *s;

			k += 32;
		}

		while (k != lastR)
		{
			result += *k ^ *s;
			k++;
		}

		s++;
	}

	return result;
}

uint64_t NestedLoop_simd_xor(Relation &R, Relation &S)
{
	// following sota nested loop implementation
	uint64_t result = 0;
	Timestamp *r, *s;
	Timestamp* lastR = R.startpoints + R.numRecords;
	Timestamp* lastS = S.startpoints + S.numRecords;

	uint32_t group_num = 32;
	//__m256i *vecA = (__m256i*) aligned_alloc( 32, group_num*sizeof(__m256i)), vecB, vecResult = _mm256_set1_epi64x(0);
	__m512i *vecA = (__m512i*) aligned_alloc( 64, group_num*sizeof(__m512i)), vecB, vecResult = _mm512_set1_epi64(0);

	s = S.startpoints;
	while (lastS - s >= group_num)
	{
		for (uint32_t i = 0; i < group_num; i++)
			//vecA[i] = _mm256_set1_epi64x(*(s+i));
			vecA[i] = _mm512_set1_epi64(*(s+i));

		r = R.startpoints;
		while (lastR - r >= 32)
		{
			for (uint32_t i = 0; i < group_num; i++)
			{
				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+0));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+8));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+16));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+24));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				/*
				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+0));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+4));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+8));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+12));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+16));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+20));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+24));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+28));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);
				*/
			}

			r += 32;
		}

		while (r < lastR)
		{
			for (uint32_t i = 0; i < group_num; i++)
			{
				result += *(s+i) ^ *r;
			}

			r++;
		}

		s += group_num;
	}

	group_num = lastS - s;
	while (s < lastS)
	{
		for (uint32_t i = 0; i < group_num; i++)
			//vecA[i] = _mm256_set1_epi64x(*(s+i));
			vecA[i] = _mm512_set1_epi64(*(s+i));

		r = R.startpoints;
		while (lastR - r >= 32)
		{
			for (uint32_t i = 0; i < group_num; i++)
			{
				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+0));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+8));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+16));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				vecB = _mm512_loadu_si512(reinterpret_cast<__m512i*>(r+24));
				vecB = _mm512_xor_si512(vecB, vecA[i]);
				vecResult = _mm512_add_epi64(vecResult,vecB);

				/*
				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+0));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+4));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+8));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+12));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+16));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+20));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+24));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);

				vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(r+28));
				vecB = _mm256_xor_si256(vecB, vecA[i]);
				vecResult = _mm256_add_epi64(vecResult,vecB);
				*/
			}

			r += 32;
		}

		while (r < lastR)
		{
			for (uint32_t i = 0; i < group_num; i++)
			{
				result += *(s+i) ^ *r;
			}

			r++;
		}

		s += group_num;
	}

	free(vecA);
	//uint64_t result_list[4];
	uint64_t result_list[8];
	//_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);
	_mm512_storeu_si512(reinterpret_cast<__m512i*>(result_list), vecResult);
	//result += result_list[0] + result_list[1] + result_list[2] + result_list[3];
	result += result_list[0] + result_list[1] + result_list[2] + result_list[3] + result_list[4] + result_list[5] + result_list[6] + result_list[7];

	return result;
}

// 1 Forward Scan for 1 big group, used for (S,E) / (E,S) MiniJoins
uint64_t bgudFS_Ends_wc(Relation& R, Relation& S, int max_group_size, BucketIndex& BI)
{
	if (R.numRecords == 0)
		return 0;

	uint64_t result = 0;

	uint32_t groups_num = R.numRecords / max_group_size;
	if (R.numRecords % max_group_size)
		groups_num++;
	uint32_t group_size;

	Timestamp** pointers_group_end;

	for (uint32_t i = 0; i < groups_num; i++)
	{
		group_size = (i == (groups_num-1)) ? R.numRecords - i * max_group_size : max_group_size;

		pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
		for (uint32_t j = 0; j < group_size; j++)
			pointers_group_end[j] = R.endpoints + i*max_group_size + j;
		std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);

		result += bgudFS_internalLoop_wc( pointers_group_end, group_size, S.startpoints, S.startpoints + S.numRecords, BI, S.minStart);

		free(pointers_group_end);
	}

	return result;
}

// 1 Forward Scan for 1 big group, used for (S,E) / (E,S) MiniJoins
uint64_t bgudFS_Ends_xor(Relation& R, Relation& S, int max_group_size, BucketIndex& BI)
{
	if (R.numRecords == 0)
		return 0;

	uint64_t result = 0;

	uint32_t groups_num = R.numRecords / max_group_size;
	if (R.numRecords % max_group_size)
		groups_num++;
	uint32_t group_size;

	Timestamp** pointers_group_start;
	Timestamp** pointers_group_end;

	for (uint32_t i = 0; i < groups_num; i++)
	{
		group_size = (i == (groups_num-1)) ? R.numRecords - i * max_group_size : max_group_size;

		pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
		pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
		for (uint32_t j = 0; j < group_size; j++)
			pointers_group_end[j] = R.endpoints + i*max_group_size + j;
		std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
		for (uint32_t j = 0; j < group_size; j++)
			pointers_group_start[j] = R.startpoints + (pointers_group_end[j] - R.endpoints);

		result += bgudFS_internalLoop_xor( pointers_group_start, pointers_group_end, group_size, S.startpoints, S.startpoints + S.numRecords, BI, S.minStart);

		free(pointers_group_start);
		free(pointers_group_end);
	}

	return result;
}

// 1 Forward Scan for 1 big group, used for  (S,E) / (E,S) MiniJoins
uint64_t bgudFS_Ends_simd_xor(Relation& R, Relation& S, int max_group_size, BucketIndex& BI)
{
	if (R.numRecords == 0)
		return 0;

	uint64_t result = 0;

	uint32_t groups_num = R.numRecords / max_group_size;
	if (R.numRecords % max_group_size)
		groups_num++;
	uint32_t group_size;

	Timestamp** pointers_group_start;
	Timestamp** pointers_group_end;

	for (uint32_t i = 0; i < groups_num; i++)
	{
		group_size = (i == (groups_num-1)) ? R.numRecords - i * max_group_size : max_group_size;

		pointers_group_start = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
		pointers_group_end   = (Timestamp**) malloc( group_size*sizeof(Timestamp*) );
		for (uint32_t j = 0; j < group_size; j++)
			pointers_group_end[j] = R.endpoints + i*max_group_size + j;
		std::sort( pointers_group_end, pointers_group_end + group_size, CompareByEndPointersT);
		for (uint32_t j = 0; j < group_size; j++)
			pointers_group_start[j] = R.startpoints + (pointers_group_end[j] - R.endpoints);

		result += bgudFS_internalLoop_simd_xor( pointers_group_start, pointers_group_end, group_size, S.startpoints, S.startpoints + S.numRecords, BI, S.minStart);

		free(pointers_group_start);
		free(pointers_group_end);
	}

	return result;
}

void* run_mini_joins_atomic(void* args)
{
	MiniJoinsStructureAtomic* mjsa = (MiniJoinsStructureAtomic*) args;

	uint64_t res = 0;

	if (mjsa->allocationStrategy != OS)
	{
		if( stick_this_thread_to_core(mjsa->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	if (mjsa->mode == WORKLOAD_COUNT)
	{
		if (mjsa->FS_est >= OPT_FS_BARRIER)
			res += bgudFS_wc( *(mjsa->sR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIR), *(mjsa->sBIS) );
		else
			res += udFS_wc( *(mjsa->sR), *(mjsa->sS));

		res += bgudFS_Ends_wc( *(mjsa->eS), *(mjsa->sR), mjsa->max_group_size, *(mjsa->sBIR) );
		res += bgudFS_Ends_wc( *(mjsa->eR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIS) );
		res += NestedLoop_wc( *(mjsa->sR), *(mjsa->cS) );
		res += NestedLoop_wc( *(mjsa->cR), *(mjsa->sS) );
	}
	else if (mjsa->mode == XOR)
	{
		if (mjsa->FS_est >= OPT_FS_BARRIER)
			res += bgudFS_xor( *(mjsa->sR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIR), *(mjsa->sBIS) );
		else
			res += udFS_xor( *(mjsa->sR), *(mjsa->sS));

		res += bgudFS_Ends_xor( *(mjsa->eS), *(mjsa->sR), mjsa->max_group_size, *(mjsa->sBIR) );
		res += bgudFS_Ends_xor( *(mjsa->eR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIS) );
		res += NestedLoop_xor( *(mjsa->sR), *(mjsa->cS) );
		res += NestedLoop_xor( *(mjsa->cR), *(mjsa->sS) );
	}
	else if (mjsa->mode == SIMD_XOR)
	{
		if (mjsa->FS_est >= OPT_FS_BARRIER)
			res += bgudFS_simd_xor( *(mjsa->sR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIR), *(mjsa->sBIS) );
		else
			res += udFS_simd_xor( *(mjsa->sR), *(mjsa->sS));

		res += bgudFS_Ends_simd_xor( *(mjsa->eS), *(mjsa->sR), mjsa->max_group_size, *(mjsa->sBIR) );
		res += bgudFS_Ends_simd_xor( *(mjsa->eR), *(mjsa->sS), mjsa->max_group_size, *(mjsa->sBIS) );
		res += NestedLoop_simd_xor( *(mjsa->sR), *(mjsa->cS) );
		res += NestedLoop_simd_xor( *(mjsa->cR), *(mjsa->sS) );
	}

	*(mjsa->result) = res;

	return NULL;
}

void* run_mini_joins_greedy3(void* args)
{
	MiniJoinsStructureGreedy3* mjsg = (MiniJoinsStructureGreedy3*) args;

//Timer tim;
//tim.start();

	uint64_t res = 0;

	if (mjsg->allocationStrategy != OS)
	{
		if( stick_this_thread_to_core(mjsg->core_id) != 0)
		{
			printf("Error with thread pinning\n");
			exit(1);
		}
	}

	if (mjsg->mode == WORKLOAD_COUNT)
	{
		for (const std::pair<uint32_t, uint32_t>& mj : *(mjsg->mjoins))
		{
			switch (mj.second)
			{
				case 0:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_wc( mjsg->psR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_wc( mjsg->psR[mj.first], mjsg->psS[mj.first]);
					break;
				case 1:
					res += bgudFS_Ends_wc( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 2:
					res += bgudFS_Ends_wc( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 3:
					res += NestedLoop_wc( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 4:
					res += NestedLoop_wc( mjsg->pcR[mj.first], mjsg->psS[mj.first] );
					break;
				default:
					printf("Unknown mini join type given\n");
					exit(1);
			}
		}
	}
	else if (mjsg->mode == XOR)
	{
		for (const std::pair<uint32_t, uint32_t>& mj : *(mjsg->mjoins))
		{
			switch (mj.second)
			{
				case 0:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_xor( mjsg->psR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_xor( mjsg->psR[mj.first], mjsg->psS[mj.first]);
					break;
				case 1:
					res += bgudFS_Ends_xor( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 2:
					res += bgudFS_Ends_xor( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 3:
					res += NestedLoop_xor( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 4:
					res += NestedLoop_xor( mjsg->pcR[mj.first], mjsg->psS[mj.first] );
					break;
				default:
					printf("Unknown mini join type given\n");
					exit(1);
			}
		}
	}
	else if (mjsg->mode == SIMD_XOR)
	{
		for (const std::pair<uint32_t, uint32_t>& mj : *(mjsg->mjoins))
		{
			switch (mj.second)
			{
				case 0:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_simd_xor( mjsg->psR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_simd_xor( mjsg->psR[mj.first], mjsg->psS[mj.first]);
					break;
				case 1:
					res += bgudFS_Ends_simd_xor( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 2:
					res += bgudFS_Ends_simd_xor( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 3:
					res += NestedLoop_simd_xor( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 4:
					res += NestedLoop_simd_xor( mjsg->pcR[mj.first], mjsg->psS[mj.first] );
					break;
				default:
					printf("Unknown mini join type given\n");
					exit(1);
			}
		}
	}

//double thread_time = tim.stop();
//printf("thread %d: %lf\n", mjsg->thread_id, thread_time);

	*(mjsg->result) = res;

	return NULL;
}

void greedy_schedule3( Relation* psR, Relation* peR, Relation* pcR, Relation* psS, Relation* peS, Relation* pcS,
	std::vector< std::pair< std::vector< std::pair<uint32_t,uint32_t> >, uint64_t > > &mini_joins, uint32_t numThreads)
{
	// vector of pair< pair<partition_id,join_type>, load > to store all jobs sorted by load
	std::vector < std::pair< std::pair<uint32_t,uint32_t>, uint64_t> > jobs_list;
	for (uint32_t i = 0; i < numThreads; i++)
	{
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 0), psR[i].numRecords * (uint64_t) psS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 1), peS[i].numRecords * (uint64_t) psR[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 2), peR[i].numRecords * (uint64_t) psS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 3), psR[i].numRecords * (uint64_t) pcS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 4), pcR[i].numRecords * (uint64_t) psS[i].numRecords ) );
	}
	
	std::sort(jobs_list.begin(), jobs_list.end(), [](const std::pair< std::pair<uint32_t,uint32_t>, uint64_t>& a, const std::pair< std::pair<uint32_t,uint32_t>, uint64_t>& b) { return a.second > b.second; } );

	// initialize thread bags with 0
	for (uint32_t i = 0; i < numThreads; i++)
		mini_joins[i].second = 0;
	std::make_heap(mini_joins.begin(), mini_joins.end(), [](const auto& a, const auto& b) { return a.second > b.second; });

	for (uint32_t i = 0; i < jobs_list.size(); i++)
	{
		if (jobs_list.at(i).second > 0)
		{
			std::pair< std::vector< std::pair<uint32_t,uint32_t> >, uint64_t > temp = mini_joins.front();
			temp.first.push_back( std::pair<uint32_t, uint32_t>( jobs_list.at(i).first.first, jobs_list.at(i).first.second) );
			temp.second += jobs_list.at(i).second;

			std::pop_heap(mini_joins.begin(), mini_joins.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
			mini_joins.pop_back();
			mini_joins.push_back(temp);
			std::push_heap(mini_joins.begin(), mini_joins.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
		}
	}
}

// main Domain-based Partitioning function
uint64_t ParallelDomainBased_Execute3(Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS, int max_group_size,
						BucketIndex *psBIR, BucketIndex *psBIS, uint32_t numThreads, int mode, uint64_t FS_est, int allocationStrategy, int* thread_core_map)
{
	// execute
	// ! we also provide atomic execution beq of NUMA awareness (greedy mixes partitions, so it needs |sockets|) !
	uint64_t result = 0;
	pthread_t thread_id[numThreads];
	uint64_t thread_results[numThreads];
	bool atomicExecution = false;

	if (atomicExecution)
	{
		MiniJoinsStructureAtomic mjsa[numThreads];
		for (uint32_t i = 0; i < numThreads; i++)
		{
			mjsa[i].sR  = &psR[i];
			mjsa[i].sS  = &psS[i];
			mjsa[i].eR  = &peR[i];
			mjsa[i].eS  = &peS[i];
			mjsa[i].cR  = &pcR[i];
			mjsa[i].cS  = &pcS[i];
			mjsa[i].max_group_size = max_group_size;
			mjsa[i].sBIR = &psBIR[i];
			mjsa[i].sBIS = &psBIS[i];

			mjsa[i].allocationStrategy = allocationStrategy;
			mjsa[i].FS_est = FS_est;
			mjsa[i].mode = mode;
			mjsa[i].thread_id = i;
			mjsa[i].core_id = thread_core_map[i];
			mjsa[i].result = &thread_results[i];

			pthread_create( &thread_id[i], NULL, run_mini_joins_atomic, (void*) &mjsa[i]);
		}
		for (uint32_t i = 0; i < numThreads; i++)
		{
			pthread_join( thread_id[i], NULL);
		}
	}
	else
	{
		// array of pair < vector<pair<partitionId,type>> , cost >
		std::vector< std::pair < std::vector< std::pair<uint32_t,uint32_t> >, uint64_t > > mini_joins(numThreads);
		greedy_schedule3( psR, peR, pcR, psS, peS, pcS, mini_joins, numThreads);

		MiniJoinsStructureGreedy3 mjsg[numThreads];
		for (uint32_t i = 0; i < numThreads; i++)
		{
			mjsg[i].psR = psR;
			mjsg[i].psS = psS;
			mjsg[i].peR = peR;
			mjsg[i].peS = peS;
			mjsg[i].pcR = pcR;
			mjsg[i].pcS = pcS;
			mjsg[i].max_group_size = max_group_size;
			mjsg[i].psBIR = psBIR;
			mjsg[i].psBIS = psBIS;

			mjsg[i].allocationStrategy = allocationStrategy;
			mjsg[i].FS_est = FS_est;
			mjsg[i].mode = mode;
			mjsg[i].mjoins = &(mini_joins.at(i).first);
			mjsg[i].thread_id = i;
			mjsg[i].core_id = thread_core_map[i];
			mjsg[i].result = &thread_results[i];

			pthread_create( &thread_id[i], NULL, run_mini_joins_greedy3, (void*) &mjsg[i]);
		}
		for (uint32_t i = 0; i < numThreads; i++)
		{
			pthread_join( thread_id[i], NULL);
		}
	}

	// add thread results
	for (uint32_t i = 0; i < numThreads; i++)
		result += thread_results[i];

	return result;
}

uint64_t domain_based3(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map)
{
	// set starting variables
	Timer tim;
	uint64_t result = 0;
	Timestamp domain_start = std::min(R.minStart, S.minStart);
	Timestamp domain_end = std::max(R.maxEnd, S.maxEnd);
	uint32_t numGranules = (numThreads < 10) ? 1000 * numThreads : 100 * numThreads;
	uint64_t granuleExtent = (Timestamp) ceil((double)(domain_end-domain_start)/numGranules);

	// partition input
	printf("-------- PRE-JOIN --------\n");
	Relation *psR, *psS, *peR, *peS, *pcR, *pcS;
	BucketIndex *psBIR, *psBIS;
	psR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	psS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	peR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	peS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pcR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pcS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	psBIR = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	psBIS = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	uint64_t FS_est = create_partitions3( R, S, psR, psS, peR, peS, pcR, pcS, psBIR, psBIS, domain_start, domain_end, numGranules, granuleExtent, numThreads, allocationStrategy, thread_core_map);
	printf("--------------------------\n");

	// join
	printf("\n--------- JOIN ---------\n");
	double joinTime;
	for (uint32_t i = 0; i < computations; i++)
	{
		tim.start();
		result = ParallelDomainBased_Execute3( psR, psS, peR, peS, pcR, pcS, max_group_size, psBIR, psBIS, numThreads, mode, FS_est, allocationStrategy, thread_core_map);
		joinTime = tim.stop();
		printf("Total Joining:			  %lf secs\n", joinTime);
	}
	printf("------------------------\n");

	// free memory
	for (uint32_t i = 0; i < numThreads; i++)
	{
		free(psBIR[i].bucket_list_decomposed);
		free(psBIS[i].bucket_list_decomposed);

		if (allocationStrategy != OS)
		{
			if ( (psR[i].numRecords*sizeof(Timestamp)) && (psR[i].startpoints) ) numa_free( psR[i].startpoints, psR[i].numRecords*sizeof(Timestamp) );
			if ( (psR[i].numRecords*sizeof(Timestamp)) && (psR[i].endpoints)   ) numa_free( psR[i].endpoints,   psR[i].numRecords*sizeof(Timestamp) );
			if ( (psS[i].numRecords*sizeof(Timestamp)) && (psS[i].startpoints) ) numa_free( psS[i].startpoints, psS[i].numRecords*sizeof(Timestamp) );
			if ( (psS[i].numRecords*sizeof(Timestamp)) && (psS[i].endpoints)   ) numa_free( psS[i].endpoints,   psS[i].numRecords*sizeof(Timestamp) );
			if ( (peR[i].numRecords*sizeof(Timestamp)) && (peR[i].startpoints) ) numa_free( peR[i].startpoints, peR[i].numRecords*sizeof(Timestamp) );
			if ( (peR[i].numRecords*sizeof(Timestamp)) && (peR[i].endpoints)   ) numa_free( peR[i].endpoints,   peR[i].numRecords*sizeof(Timestamp) );
			if ( (peS[i].numRecords*sizeof(Timestamp)) && (peS[i].startpoints) ) numa_free( peS[i].startpoints, peS[i].numRecords*sizeof(Timestamp) );
			if ( (peS[i].numRecords*sizeof(Timestamp)) && (peS[i].endpoints)   ) numa_free( peS[i].endpoints,   peS[i].numRecords*sizeof(Timestamp) );
			if ( (pcR[i].numRecords*sizeof(Timestamp)) && (pcR[i].startpoints) ) numa_free( pcR[i].startpoints, pcR[i].numRecords*sizeof(Timestamp) );
			if ( (pcR[i].numRecords*sizeof(Timestamp)) && (pcR[i].endpoints)   ) numa_free( pcR[i].endpoints,   pcR[i].numRecords*sizeof(Timestamp) );
			if ( (pcS[i].numRecords*sizeof(Timestamp)) && (pcS[i].startpoints) ) numa_free( pcS[i].startpoints, pcS[i].numRecords*sizeof(Timestamp) );
			if ( (pcS[i].numRecords*sizeof(Timestamp)) && (pcS[i].endpoints)   ) numa_free( pcS[i].endpoints,   pcS[i].numRecords*sizeof(Timestamp) );
		}
		else
		{
			free(psR[i].startpoints);
			free(psS[i].startpoints);
			free(peR[i].startpoints);
			free(peS[i].startpoints);
			free(pcR[i].startpoints);
			free(pcS[i].startpoints);
			free(psR[i].endpoints);
			free(psS[i].endpoints);
			free(peR[i].endpoints);
			free(peS[i].endpoints);
			free(pcR[i].endpoints);
			free(pcS[i].endpoints);
		}

		free(psR[i].record_list);
		free(psS[i].record_list);
		free(peR[i].record_list);
		free(peS[i].record_list);
		free(pcR[i].record_list);
		free(pcS[i].record_list);
	}
	free(psBIR);
	free(psBIS);
	free(psR);
	free(psS);
	free(peR);
	free(peS);
	free(pcR);
	free(pcS);

	return result;
}
