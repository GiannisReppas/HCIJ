#include "../def.hpp"
#include "../containers/relation.hpp"

void SetBoundaries(Relation& R, Relation& S, uint32_t numPartitions, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t* partition_sizes);
uint64_t udFS_internalLoop_estimate( Timestamp* curr, Timestamp* firstFS, Timestamp* lastFS);

uint64_t FS_estimation(Relation& R, Relation&S)
{
	if ( (R.numRecords < 1000) && (S.numRecords < 1000) )
		return 0;

	// create boundaries
	uint32_t numPartitions = 50;
	uint32_t* partition_sizes = (uint32_t*) malloc( numPartitions   * sizeof(uint32_t) );
	Timestamp** boundariesR = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
	Timestamp** boundariesS = (Timestamp**) malloc((numPartitions+1)* sizeof(Timestamp*));
	SetBoundaries( R, S, numPartitions, boundariesR, boundariesS, partition_sizes);

	// execute udFSs for 1/10000 of total FSs of each partition, counting workload count using udFS
	uint64_t result = 0;
	Timestamp *r, *s;
	Timestamp* total_lastR = boundariesR[numPartitions] + 1;
	Timestamp* total_lastS = boundariesS[numPartitions] + 1;
	uint32_t total_FSs_executed = 0;
	uint32_t division = 10000;

	for (uint32_t i = 0; i < numPartitions; i++)
	{
		partition_sizes[i] = ceil( partition_sizes[i] / (float) division );

		r = boundariesR[i]+1;
		s = boundariesS[i]+1;

		total_FSs_executed += partition_sizes[i];

		while ( (partition_sizes[i] != 0) && (r != boundariesR[i+1]+1) && (s != boundariesS[i+1]+1) )
		{
			if (*r < *s)
			{
				result += udFS_internalLoop_estimate( R.endpoints + (r - R.startpoints), s, total_lastS);

				r++;
			}
			else
			{
				result += udFS_internalLoop_estimate( S.endpoints + (s - S.startpoints), r, total_lastR);

				s++;
			}

			partition_sizes[i]--;
		}

		if (s == boundariesS[i+1]+1)
		{
			while ( (partition_sizes[i] != 0) && (r < boundariesR[i+1]+1) )
			{
				result += udFS_internalLoop_estimate( R.endpoints + (r - R.startpoints), s, total_lastS);
				r++;

				partition_sizes[i]--;
			}
		}
		else
		{
			while ( (partition_sizes[i] != 0) && (s < boundariesS[i+1]+1) )
			{
				result += udFS_internalLoop_estimate( S.endpoints + (s - S.startpoints), r, total_lastR);
				s++;

				partition_sizes[i]--;
			}
		}
	}

	// get average extent
	result = result / total_FSs_executed;

	// free memory used
	free(boundariesR);
	free(boundariesS);
	free(partition_sizes);

	return result;
}

/*
void SetBoundaries(Relation& R, Relation& S, uint32_t numPartitions, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t* partition_sizes)
{
	Timestamp* currR = R.startpoints;
	Timestamp* currS = S.startpoints;
	Timestamp* endR = R.startpoints + R.numRecords;
	Timestamp* endS = S.startpoints + S.numRecords;
	boundariesR[0] = currR - 1;
	boundariesS[0] = currS - 1;

	// set how many tuples each partition will have (there may be differences of 1 tuple in size)
	uint32_t total_size = R.numRecords + S.numRecords;
	for (uint32_t i = 0; i < numPartitions; i++)
	{
		partition_sizes[i] = total_size / numPartitions;
		if (i < (total_size % numPartitions))
			partition_sizes[i] += 1;
	}

	// plane sweep the relations, set boundaries when you read enough elements and restart
	uint32_t curr_size = 0;
	uint32_t curr_partition = 0;
	while ( (currR != endR) && (currS != endS) )
	{
		(*currR < *currS) ? currR++ : currS++;
		curr_size++;

		if (curr_size == partition_sizes[ curr_partition ])
		{
			boundariesR[ curr_partition+1 ] = currR - 1;
			boundariesS[ curr_partition+1 ] = currS - 1;

			//printf("Partition %d ends at [%ld,%ld) and [%ld,%ld)\n", curr_partition, *boundariesR[curr_partition+1], *boundariesR[curr_partition+1], *boundariesS[curr_partition+1], *boundariesS[curr_partition+1]);

			curr_size = 0;
			curr_partition++;
		}
	}

	// set remaining boundary element for current partition (during which the plane sweep ended)
	Timestamp* curr = (currR == endR) ? currS : currR;
	Timestamp* end = (currR == endR) ? endS : endR ;
	while (curr_size != partition_sizes[ curr_partition ])
	{
		curr++;
		curr_size++;
	}
	if (currR == endR)
	{
		boundariesR[ curr_partition+1 ] = currR - 1;
		boundariesS[ curr_partition+1 ] = curr - 1;
	}
	else
	{
		boundariesR[ curr_partition+1 ] = curr - 1;
		boundariesS[ curr_partition+1 ] = currS - 1;
	}

	//printf("Partition %d ends at [%ld,%ld) and [%ld,%ld)\n", curr_partition, *boundariesR[curr_partition+1], *boundariesR[curr_partition+1], *boundariesS[curr_partition+1], *boundariesS[curr_partition+1]);

	curr_partition++;

	// set remaining boundary elements for the rest of the partitions that will contain elemenets from only 1 relation (if needed)
	while (curr_partition != numPartitions)
	{
		curr += partition_sizes[ curr_partition ];
		if (currR == endR)
		{
			boundariesR[ curr_partition+1 ] = currR - 1;
			boundariesS[ curr_partition+1 ] = curr - 1;
		}
		else
		{
			boundariesR[ curr_partition+1 ] = curr - 1;
			boundariesS[ curr_partition+1 ] = currS - 1;
		}

		//printf("Partition %d ends at [%ld,%ld) and [%ld,%ld)\n", curr_partition, *boundariesR[curr_partition+1], *boundariesR[curr_partition+1], *boundariesS[curr_partition+1], *boundariesS[curr_partition+1]);

		curr_partition++;
	}
}
*/

void SetBoundaries(Relation& R, Relation& S, uint32_t numPartitions, Timestamp** boundariesR, Timestamp** boundariesS, uint32_t* partition_sizes)
{
	uint32_t total_size = R.numRecords + S.numRecords;

	boundariesR[0] = R.startpoints - 1;
	boundariesS[0] = S.startpoints - 1;

	// Distribute partition sizes evenly
	for (uint32_t i = 0; i < numPartitions; i++)
	{
		partition_sizes[i] = total_size / numPartitions;
		if (i < (total_size % numPartitions))
			partition_sizes[i] += 1;
	}

	uint32_t k = 0;
	
	// Perform a binary search for each boundary (except the last)
	for (uint32_t p = 0; p < numPartitions - 1; p++)
	{
		k += partition_sizes[p];

		// We want to find index i in R and j in S such that:
		// i + j = k  AND  R[i-1] <= S[j]  AND  S[j-1] <= R[i]
		int64_t low = std::max(static_cast<int64_t>(0), static_cast<int64_t>(k) - static_cast<int64_t>(S.numRecords));
		int64_t high = std::min(static_cast<int64_t>(k), static_cast<int64_t>(R.numRecords));
		
		uint32_t i = 0;
		uint32_t j = 0;
		
		while (low <= high)
		{
			i = low + (high - low) / 2;
			j = k - i;

			// Handle array extremes
			Timestamp r_i_minus_1 = (i == 0) ? std::numeric_limits<Timestamp>::min() : *(R.startpoints + i - 1);
			Timestamp s_j_minus_1 = (j == 0) ? std::numeric_limits<Timestamp>::min() : *(S.startpoints + j - 1);
			Timestamp r_i = (i == R.numRecords) ? std::numeric_limits<Timestamp>::max() : *(R.startpoints + i);
			Timestamp s_j = (j == S.numRecords) ? std::numeric_limits<Timestamp>::max() : *(S.startpoints + j);

			// Merge Path intersection condition
			if (r_i_minus_1 <= s_j && s_j_minus_1 <= r_i)
				break;
			else if (r_i_minus_1 > s_j)
				high = i - 1; // move left
			else
				low = i + 1; // move right
		}
		
		// Lock in the boundaries for this partition
		boundariesR[p + 1] = R.startpoints + i - 1;
		boundariesS[p + 1] = S.startpoints + j - 1;
	}

	// The final boundaries are simply the end of both relations
	boundariesR[numPartitions] = R.startpoints + R.numRecords - 1;
	boundariesS[numPartitions] = S.startpoints + S.numRecords - 1;
}

uint64_t udFS_internalLoop_estimate( Timestamp* curr, Timestamp* firstFS, Timestamp* lastFS)
{
	Timestamp* pivot = firstFS;

	while ( ((pivot+32) < lastFS) && (*(pivot+31) < *curr) )
	{
		pivot += 32;
	}

	while ( (pivot != lastFS) && (*pivot < *curr) )
	{
		pivot++;
	}

	uint64_t result = (uint64_t) (pivot - firstFS);

	return result;
}
