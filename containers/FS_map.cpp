#include "FS_map.hpp"

bool CompareByLoad(const FSGrouped& a, const FSGrouped& b)
{
	uint32_t load_a = 0;
	for (uint32_t j = 0; j < a.group_size; j++)
	{
		load_a += a.end[j] - a.start;
	}

	uint32_t load_b = 0;
	for (uint32_t j = 0; j < b.group_size; j++)
	{
		load_b += b.end[j] - b.start;
	}

	return load_a < load_b;
}

#define xor_unroll(result, start_ptr, pivot) \
	result += *(start_ptr) ^ *(pivot+0); \
	result += *(start_ptr) ^ *(pivot+1); \
	result += *(start_ptr) ^ *(pivot+2); \
	result += *(start_ptr) ^ *(pivot+3); \
	result += *(start_ptr) ^ *(pivot+4); \
	result += *(start_ptr) ^ *(pivot+5); \
	result += *(start_ptr) ^ *(pivot+6); \
	result += *(start_ptr) ^ *(pivot+7); \
	result += *(start_ptr) ^ *(pivot+8); \
	result += *(start_ptr) ^ *(pivot+9); \
	result += *(start_ptr) ^ *(pivot+10); \
	result += *(start_ptr) ^ *(pivot+11); \
	result += *(start_ptr) ^ *(pivot+12); \
	result += *(start_ptr) ^ *(pivot+13); \
	result += *(start_ptr) ^ *(pivot+14); \
	result += *(start_ptr) ^ *(pivot+15); \
	result += *(start_ptr) ^ *(pivot+16); \
	result += *(start_ptr) ^ *(pivot+17); \
	result += *(start_ptr) ^ *(pivot+18); \
	result += *(start_ptr) ^ *(pivot+19); \
	result += *(start_ptr) ^ *(pivot+20); \
	result += *(start_ptr) ^ *(pivot+21); \
	result += *(start_ptr) ^ *(pivot+22); \
	result += *(start_ptr) ^ *(pivot+23); \
	result += *(start_ptr) ^ *(pivot+24); \
	result += *(start_ptr) ^ *(pivot+25); \
	result += *(start_ptr) ^ *(pivot+26); \
	result += *(start_ptr) ^ *(pivot+27); \
	result += *(start_ptr) ^ *(pivot+28); \
	result += *(start_ptr) ^ *(pivot+29); \
	result += *(start_ptr) ^ *(pivot+30); \
	result += *(start_ptr) ^ *(pivot+31);

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

/**************************************************************************************************/

FSDetached::FSDetached()
{
}

FSDetached::~FSDetached()
{
}

/**************************************************************************************************/

FSMapDetached::FSMapDetached()
{
	this->result_list = NULL;
	this->numResults = 0;
	this->load = 0;
}

uint64_t FSMapDetached::compute_results_wc(uint32_t j)
{
	Timestamp* curr = this->result_list[j].curr;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp* end = this->result_list[j].end;
	uint64_t result = 0;

	while ((pivot+32) < end)
	{
		result += 32;

		pivot += 32;
	}

	while (pivot != end)
	{
		result += 1;

		pivot++;
	}

	return result;
}

//__attribute__((optimize("no-tree-vectorize")))
uint64_t FSMapDetached::compute_results_xor(uint32_t j)
{
	Timestamp* curr = this->result_list[j].curr;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp* end = this->result_list[j].end;
	uint64_t result = 0;

	while ((pivot+32) < end)
	{
		result += *curr ^ *(pivot+0);
		result += *curr ^ *(pivot+1);
		result += *curr ^ *(pivot+2);
		result += *curr ^ *(pivot+3);
		result += *curr ^ *(pivot+4);
		result += *curr ^ *(pivot+5);
		result += *curr ^ *(pivot+6);
		result += *curr ^ *(pivot+7);
		result += *curr ^ *(pivot+8);
		result += *curr ^ *(pivot+9);
		result += *curr ^ *(pivot+10);
		result += *curr ^ *(pivot+11);
		result += *curr ^ *(pivot+12);
		result += *curr ^ *(pivot+13);
		result += *curr ^ *(pivot+14);
		result += *curr ^ *(pivot+15);
		result += *curr ^ *(pivot+16);
		result += *curr ^ *(pivot+17);
		result += *curr ^ *(pivot+18);
		result += *curr ^ *(pivot+19);
		result += *curr ^ *(pivot+20);
		result += *curr ^ *(pivot+21);
		result += *curr ^ *(pivot+22);
		result += *curr ^ *(pivot+23);
		result += *curr ^ *(pivot+24);
		result += *curr ^ *(pivot+25);
		result += *curr ^ *(pivot+26);
		result += *curr ^ *(pivot+27);
		result += *curr ^ *(pivot+28);
		result += *curr ^ *(pivot+29);
		result += *curr ^ *(pivot+30);
		result += *curr ^ *(pivot+31);

		pivot += 32;
	}

	while (pivot != end)
	{
		result += *curr ^ *pivot;

		pivot++;
	}

	return result;
}

uint64_t FSMapDetached::compute_results_simd_xor(uint32_t j)
{
	Timestamp* curr = this->result_list[j].curr;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp* end = this->result_list[j].end;
	uint64_t result = 0;

	__m256i vecA, vecB, vecResult;
	Timestamp helper_list[4];
	helper_list[0] = *curr;
	helper_list[1] = *curr;
	helper_list[2] = *curr;
	helper_list[3] = *curr;
	vecA = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(helper_list));
	uint64_t result_list[4] = {0,0,0,0};

	while ((pivot+32) < end)
	{
		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+0));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+4));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+8));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+12));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+16));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+20));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+24));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		vecResult = _mm256_loadu_si256(reinterpret_cast<__m256i*>(result_list));
		vecB = _mm256_loadu_si256(reinterpret_cast<__m256i*>(pivot+28));
		vecB = _mm256_xor_si256(vecB, vecA);
		vecResult = _mm256_add_epi64(vecResult,vecB);
		_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);

		pivot += 32;
	}

	while (pivot != end)
	{
		result += *curr ^ *pivot;

		pivot++;
	}

	result += result_list[0] + result_list[1] + result_list[2] + result_list[3];

	return result;
}

FSMapDetached::~FSMapDetached()
{
	free(this->result_list);
}


/**************************************************************************************************/

FSGrouped::FSGrouped()
{
}

FSGrouped::~FSGrouped()
{
}

/**************************************************************************************************/

FSMapGrouped::FSMapGrouped()
{
	this->result_list = NULL;
	this->numResults = 0;
	this->load = 0;
}

uint64_t FSMapGrouped::compute_results_wc(uint32_t j)
{
	Timestamp** pointers_group_start = this->result_list[j].pointers_group_start;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp** end = this->result_list[j].end;
	uint32_t group_size = this->result_list[j].group_size;

	uint64_t result = 0;
	Timestamp *last;
	Timestamp** lastG = end + group_size;
	for (Timestamp **curr_end = end, **curr_start = pointers_group_start; curr_end != lastG; curr_end++, curr_start++)
	{
		last = *curr_end;
		while (last-pivot >= 32)
		{
			for (Timestamp** k = curr_end; k != lastG; k++)
			{
				result += 32;
			}

			pivot += 32;
		}

		while (pivot < last)
		{
			for (Timestamp** k = curr_end; k != lastG; k++)
			{
				result += 1;
			}

			pivot++;
		}
	}

	return result;
}

//__attribute__((optimize("no-tree-vectorize")))
uint64_t FSMapGrouped::compute_results_xor(uint32_t j)
{
	Timestamp** pointers_group_start = this->result_list[j].pointers_group_start;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp** end = this->result_list[j].end;
	uint32_t group_size = this->result_list[j].group_size;

	Timestamp local_starts[group_size];
	for (uint32_t i = 0; i < group_size; i++)
		local_starts[i] = *(pointers_group_start[i]);

	uint64_t result = 0;
	Timestamp *last;

	Timestamp **lastG_end = end + group_size;
	Timestamp *lastG_start = local_starts + group_size;

	Timestamp *curr_start = local_starts;

	for (Timestamp **curr_end = end; curr_end != lastG_end; curr_end++, curr_start++)
	{
		last = *curr_end;
		while (last - pivot >= 32)
		{
			// The compiler's hardware prefetcher will easily recognize this linear access
			for (Timestamp* k = curr_start; k != lastG_start; k++)
			{
				xor_unroll(result, k, pivot);
			}

			pivot += 32;
		}

		while (pivot < last)
		{
			for (Timestamp* k = curr_start; k != lastG_start; k++)
			{
				result += *k ^ *(pivot + 0);
			}

			pivot++;
		}
	}

	return result;
}

/*
uint64_t FSMapGrouped::compute_results_simd_xor(uint32_t j)
{
	Timestamp** pointers_group_start = this->result_list[j].pointers_group_start;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp** end = this->result_list[j].end;
	uint32_t group_size = this->result_list[j].group_size;

	__m256i *vecA = (__m256i*) aligned_alloc( 32, sizeof(__m256i) * group_size);
	__m256i vecB;
	__m256i vecResult;
	Timestamp helper_list[group_size][4];
	for (uint32_t i = 0; i < group_size; i++)
	{
		helper_list[i][0] = *(*(pointers_group_start+i));
		helper_list[i][1] = *(*(pointers_group_start+i));
		helper_list[i][2] = *(*(pointers_group_start+i));
		helper_list[i][3] = *(*(pointers_group_start+i));
		vecA[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(helper_list[i]));
	}
	uint64_t result_list[4] = {0,0,0,0};
	vecResult = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(result_list));

	uint32_t group_index = 0;
	uint64_t result = 0;
	uint32_t bufferSize;
	Timestamp *last;
	Timestamp **lastG_end = end + group_size, **lastG_start = pointers_group_start + group_size;
	for (Timestamp **curr_end = end, **curr_start = pointers_group_start; curr_end != lastG_end; curr_end++, curr_start++, group_index++)
	{
		bufferSize = lastG_end - curr_end;

		last = *curr_end;
		while (last-pivot >= 32)
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				simd_xor_unroll(vecResult, vecB, pivot, vecA, group_index, i);
			}

			pivot += 32;
		}

		while (pivot < last)
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += *(*(curr_start+i)) ^ *(pivot+0);
			}

			pivot++;
		}
	}

	_mm256_storeu_si256(reinterpret_cast<__m256i*>(result_list), vecResult);
	result += result_list[0] + result_list[1] + result_list[2] + result_list[3];

	free(vecA);

	return result;
}
*/

uint64_t FSMapGrouped::compute_results_simd_xor(uint32_t j)
{
	Timestamp** pointers_group_start = this->result_list[j].pointers_group_start;
	Timestamp* pivot = this->result_list[j].start;
	Timestamp** end = this->result_list[j].end;
	uint32_t group_size = this->result_list[j].group_size;

	__m512i *vecA = (__m512i*) aligned_alloc( 64, sizeof(__m512i) * group_size), vecB, vecResult;
	Timestamp helper_list[group_size][8];
	for (uint32_t i = 0; i < group_size; i++)
	{
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
	uint64_t result = 0;
	uint32_t bufferSize;
	Timestamp *last;
	Timestamp **lastG_end = end + group_size, **lastG_start = pointers_group_start + group_size;
	for (Timestamp **curr_end = end, **curr_start = pointers_group_start; curr_end != lastG_end; curr_end++, curr_start++, group_index++)
	{
		bufferSize = lastG_end - curr_end;

		last = *curr_end;
		while (last-pivot >= 32)
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				simd_xor_unroll(vecResult, vecB, pivot, vecA, group_index, i);
			}

			pivot += 32;
		}

		while (pivot < last)
		{
			for (uint32_t i = 0; i < bufferSize; i++)
			{
				result += *(*(curr_start+i)) ^ *(pivot+0);
			}

			pivot++;
		}
	}

	_mm512_storeu_si512(reinterpret_cast<__m512i*>(result_list), vecResult);
	result += result_list[0] + result_list[1] + result_list[2] + result_list[3] + result_list[4] + result_list[5] + result_list[6] + result_list[7];

	free(vecA);

	return result;
}
uint64_t FSMapGrouped::compute_results_rectangle_simd_xor(uint32_t start_group, uint32_t group_count)
{
	uint32_t end_group = start_group + group_count;
	Timestamp* pivot = nullptr;

	uint32_t total_tuples = 0;
	for (uint32_t g = start_group; g < end_group; g++)
		total_tuples += this->result_list[g].group_size;

	__m512i *vecA = (__m512i*) aligned_alloc(64, sizeof(__m512i) * total_tuples), vecB, vecResult;
	Timestamp *active_vals = (Timestamp*) malloc(sizeof(Timestamp) * total_tuples);
	Timestamp **active_ends = (Timestamp**) malloc(sizeof(Timestamp*) * total_tuples);
	uint64_t result_list_arr[8] = {0,0,0,0,0,0,0,0};
	vecResult = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(result_list_arr));

	uint64_t result = 0;
	uint32_t active_count = 0;
	uint32_t curr_group = start_group;
	bool batch_sig = false;

	while (active_count > 0 || curr_group < end_group)
	{
		// 1. Fast-forward the pivot AND lock the relation signature when empty
		if (active_count == 0) 
		{
			pivot = this->result_list[curr_group].start;
			batch_sig = (pivot < this->result_list[curr_group].pointers_group_start[0]);
		}

		// Add groups that start at the pivot AND target the same relation
		while (curr_group < end_group)
		{
			bool curr_sig = (this->result_list[curr_group].start < this->result_list[curr_group].pointers_group_start[0]);
			
			// Break instantly if the relation switches or if we haven't reached the start point
			if (curr_sig != batch_sig) break;
			if (this->result_list[curr_group].start > pivot) break;

			uint32_t g_size = this->result_list[curr_group].group_size;
			Timestamp** p_start = this->result_list[curr_group].pointers_group_start;
			Timestamp** p_end = this->result_list[curr_group].end;

			for (uint32_t i = 0; i < g_size; i++)
			{
				if (pivot < p_end[i]) 
				{
					Timestamp val = *(p_start[i]);
					active_vals[active_count] = val;
					
					Timestamp helper[8] = {val, val, val, val, val, val, val, val};
					vecA[active_count] = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(helper));
					
					active_ends[active_count] = p_end[i];
					active_count++;
				}
			}
			curr_group++;
		}

		// SHRINK
		for (uint32_t i = 0; i < active_count; )
		{
			if (pivot >= active_ends[i])
			{
				active_count--;
				vecA[i] = vecA[active_count];
				active_vals[i] = active_vals[active_count];
				active_ends[i] = active_ends[active_count];
			}
			else
			{
				i++;
			}
		}

		if (active_count == 0 && curr_group == end_group) break;
		if (active_count == 0) continue; 

		// FIND THE RECTANGLE BOUNDARY (pick only from the same relation)
		Timestamp* next_boundary = nullptr;
		if (curr_group < end_group)
		{
			bool curr_sig = (this->result_list[curr_group].start < this->result_list[curr_group].pointers_group_start[0]);
			if (curr_sig == batch_sig) 
			{
				next_boundary = this->result_list[curr_group].start;
			}
		}
		
		for (uint32_t i = 0; i < active_count; i++)
		{
			Timestamp* depletion_point = active_ends[i];
			if (next_boundary == nullptr || depletion_point < next_boundary)
				next_boundary = depletion_point;
		}

		// SCAN THE RECTANGLE
		while (next_boundary - pivot >= 32)
		{
			for (uint32_t i = 0; i < active_count; i++)
			{
				simd_xor_unroll(vecResult, vecB, pivot, vecA, 0, i);
			}
			pivot += 32;
		}

		while (pivot < next_boundary)
		{
			for (uint32_t i = 0; i < active_count; i++)
			{
				result += active_vals[i] ^ *pivot;
			}
			pivot++;
		}
	}

	_mm512_storeu_si512(reinterpret_cast<__m512i*>(result_list_arr), vecResult);
	result += result_list_arr[0] + result_list_arr[1] + result_list_arr[2] + result_list_arr[3] + result_list_arr[4] + result_list_arr[5] + result_list_arr[6] + result_list_arr[7];

	free(vecA);
	free(active_vals);
	free(active_ends);

	return result;
}

FSMapGrouped::~FSMapGrouped()
{
	for (uint32_t i = 0; i < this->numResults; i++)
	{
		free(this->result_list[i].pointers_group_start);
		free(this->result_list[i].end);
	}
	free(this->result_list);
}
