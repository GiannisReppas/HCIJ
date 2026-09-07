#ifndef _FS_MAP_H_
#define _FS_MAP_H_

#include "../def.hpp"
#include "relation.hpp"

/**************************************************************************************************/

class FSDetached
{
public:
	Timestamp* curr;
	Timestamp* start;
	Timestamp* end;

	FSDetached();
	~FSDetached();
};

/**************************************************************************************************/

class FSMapDetached
{
public:
	FSDetached* result_list;
	uint32_t numResults;
	uint64_t load;

	FSMapDetached();
	bool operator < (const FSMapDetached& rhs)
	{
		return (this->load < rhs.load);
	}
	uint64_t compute_results_wc(uint32_t j);
	uint64_t compute_results_xor(uint32_t j);
	uint64_t compute_results_simd_xor(uint32_t j);
	~FSMapDetached();
};

/**************************************************************************************************/

class FSGrouped
{
public:
	Timestamp** pointers_group_start;
	Timestamp* start;
	Timestamp** end;
	uint32_t group_size;

	FSGrouped();
	~FSGrouped();
};

/**************************************************************************************************/

class FSMapGrouped
{
public:
	FSGrouped* result_list;
	uint32_t numResults;
	uint64_t load;

	FSMapGrouped();
	bool operator < (const FSMapGrouped& rhs)
	{
		return (this->load < rhs.load);
	}
	uint64_t compute_results_wc(uint32_t j);
	uint64_t compute_results_xor(uint32_t j);
	uint64_t compute_results_simd_xor(uint32_t j);
	uint64_t compute_results_rectangle_simd_xor(uint32_t j, uint32_t X);
	~FSMapGrouped();
};

#endif
