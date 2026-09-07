#ifndef _BUCKET_INDEX_H_
#define _BUCKET_INDEX_H_

#include "../def.hpp"
#include "relation.hpp"

class BucketIndex
{
public:
	Timestamp** bucket_list_decomposed;
	int numBuckets;
	uint64_t bucket_range;

	BucketIndex();
	void build(Relation &R, int numBuckets);
	~BucketIndex();
};

#endif