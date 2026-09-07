#include "bucket_index.hpp"

int get_closest_node(int core);
int get_most_distant_node(int core);

BucketIndex::BucketIndex()
{
	this->bucket_list_decomposed = NULL;
}

void BucketIndex::build(Relation &R, int numBuckets)
{
	int cbucket_id = 0, btmp;
	Timestamp* i = R.startpoints;
	Timestamp* lastI = R.startpoints + R.numRecords;
	Timestamp ms = R.maxStart;

	if (R.minStart == R.maxStart)
		ms += 1;

	this->numBuckets = numBuckets;
	this->bucket_range = (Timestamp)ceil((double)(ms-R.minStart)/this->numBuckets);
	this->bucket_list_decomposed = (Timestamp**) malloc( this->numBuckets*sizeof(Timestamp*) );

	for (int i = 0; i < this->numBuckets; i++)
		this->bucket_list_decomposed[i] = lastI;

	while (i != lastI)
	{
		btmp = ceil((double)(*i - R.minStart)/this->bucket_range);
		if (btmp >= this->numBuckets)
			btmp = this->numBuckets-1;

		if (cbucket_id != btmp)
		{
			this->bucket_list_decomposed[cbucket_id] = i;
			cbucket_id++;
		}
		else
		{
			++i;
		}
	}
	this->bucket_list_decomposed[this->numBuckets-1] = lastI;
	for (int i = this->numBuckets-2; i >= 0; i--)
	{
		if (this->bucket_list_decomposed[i] == lastI)
			this->bucket_list_decomposed[i] = this->bucket_list_decomposed[i+1];
	}
}

BucketIndex::~BucketIndex()
{
	free( this->bucket_list_decomposed );
}