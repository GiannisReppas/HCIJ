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

static bool CompareByEndPointersT( Timestamp*& a, Timestamp*& b)
{
	return *a < *b;
}

#endif
