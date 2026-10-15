#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def.hpp"

/**************************************************************************************************/

class Record
{
public:
	Timestamp start;
	Timestamp end;

	Record();
	Record(Timestamp start, Timestamp end);
	~Record();
};

/**************************************************************************************************/

class Relation;
struct LoadRelationStructure
{
	Relation* rel;
	const char* filename;
	int allocationStrategy;
};

class Relation
{
public:
	Record* record_list;
	Timestamp* startpoints;
	Timestamp* endpoints;
	uint32_t numRecords;
	Timestamp minStart, maxStart, minEnd, maxEnd;
	int allocationStrategy;

	Relation();
	void load(const char *filename);
	static void *load_helper(void* args)
	{
		LoadRelationStructure *gained = (LoadRelationStructure*) args;

		gained->rel->allocationStrategy = gained->allocationStrategy;
		gained->rel->load(gained->filename);

		return NULL;
	}
	void columnStoreSwitch(int core_id);
	~Relation();
};

/**************************************************************************************************/
// Sorting conditions, defined as function objects so they always inline into sort calls.

static bool CompareByEndPointersT( Timestamp*& a, Timestamp*& b)
{
	return *a < *b;
}

/*
normally its just return a.start < b.start
but we wanted to preserve the order of equal elements for for testing purposes
*/
struct CompareByStart
{
        bool operator()(const Record& a, const Record& b) const noexcept
        {
                if (a.start < b.start)
                        return true;
                else if (a.start == b.start)
                        return a.end < b.end;

                return false;
        }
};

struct CompareByEnd
{
        bool operator()(const Record& a, const Record& b) const noexcept
        {
                return a.end < b.end;
        }
};

#endif
