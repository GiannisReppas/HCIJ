#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"

uint64_t FS_estimation(Relation& R, Relation& S);

/*
helper function
for a range of [1,size], sets toTakeStart and toTakeEnd with equal subrange to be read, for object with id chunk out of c total objects
*/
void chunk_to_read4(uint32_t size, uint32_t c, uint32_t chunk, uint32_t &toTakeStart, uint32_t &toTakeEnd)
{
	uint32_t divResult = size / c;
	uint32_t modResult = size % c;
	toTakeStart = 1;
	for (uint32_t i=0; i < chunk; i++)
	{
		if (modResult <= i)
			toTakeStart += divResult;
		else
			toTakeStart += divResult+1;
	}
	if (modResult <= chunk)
		toTakeEnd = toTakeStart + divResult;
	else
		toTakeEnd = toTakeStart + divResult+1;
	toTakeEnd--;

	return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
struct BuildStartHistogramsStructure4
{
	Relation* relR;
	Relation* relS;

	uint32_t* shistR;
	uint32_t* shistS;

	Timestamp domain_start;
	Timestamp domain_end;
	uint64_t granuleExtent;
	uint32_t numGranules;

	uint32_t thread_id;
	uint32_t numThreads;
};

void* BuildStartHistograms4(void* args)
{
	BuildStartHistogramsStructure4* bshs = (BuildStartHistogramsStructure4*) args;

	uint32_t j;
	uint32_t toTakeStart, toTakeEnd;

	chunk_to_read4( bshs->relR->numRecords, bshs->numThreads, bshs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t i = toTakeStart-1; i < toTakeEnd; i++)
	{
		j= ((*(bshs->relR)).record_list[i].start == 0) ? 0 : (*(bshs->relR)).record_list[i].start / bshs->granuleExtent;
		if ((*(bshs->relR)).record_list[i].start == bshs->domain_end)
			j =  bshs->numGranules - 1;

		bshs->shistR[j]++;
	}

	chunk_to_read4( bshs->relS->numRecords, bshs->numThreads, bshs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t i = toTakeStart-1; i < toTakeEnd; i++)
	{
		j= ((*(bshs->relS)).record_list[i].start == 0) ? 0 : (*(bshs->relS)).record_list[i].start / bshs->granuleExtent;
		if ((*(bshs->relS)).record_list[i].start == bshs->domain_end)
			j =  bshs->numGranules - 1;

		bshs->shistS[j]++;
	}

	return NULL;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

inline void AdaptivePartitioning4(uint32_t* shistR, uint32_t* shistS, uint64_t* boundaries, Timestamp domain_end, Timestamp granuleExtent, uint32_t numGranules, int numPartitions)
{
	int *sbboundaries;
	uint64_t *curr, *curr_sR, *curr_sS;
	sbboundaries = (int*) malloc(numPartitions*sizeof(int));
	curr    = (uint64_t*) malloc(numPartitions*sizeof(uint64_t));
	curr_sR = (uint64_t*) malloc(numPartitions*sizeof(uint64_t));
	curr_sS = (uint64_t*) malloc(numPartitions*sizeof(uint64_t));
	for (uint32_t i = 0; i < numPartitions; i++)
	{
		sbboundaries[i] = 0;
		curr[i] = 0;
		curr_sR[i] = 0;
		curr_sS[i] = 0;
	}
	int curr_part = 0, k = 0;
	int numGranuelsPerPartition = numGranules/numPartitions;
	auto prev_sp = -1;
	auto checked = 0, updated = 0, prev_updated = 0;

	// Compute initial load per partition.
	for (int p = 0; p < numPartitions; p++)
	{
		for (int g = 0; g < numGranuelsPerPartition; g++)
		{
			auto k = p*numGranuelsPerPartition + g;
			curr_sR[p] += shistR[k];
			curr_sS[p] += shistS[k];
		}
		boundaries[p] = ((p+1)*numGranuelsPerPartition)*granuleExtent;
		sbboundaries[p] = p*numGranuelsPerPartition;
		curr[p] = curr_sR[p]*curr_sS[p];
	}
	boundaries[numPartitions-1] = domain_end;

	std::vector<std::pair<int, size_t> > Q;
	auto idx = -1;
	while (checked-updated < numPartitions)
	{
		int sp = 0, cp = 0;
		if (idx == -1)
		{
			Q.clear();
			for (int p = 0; p < numPartitions; p++)
				Q.push_back(std::make_pair(p, curr[p]));
			sort(Q.begin(), Q.end(), [](const std::pair<int, size_t>& lhs, const std::pair<int, size_t>& rhs){ return (rhs.second > lhs.second); } );
			idx = numPartitions-1;
		}
		sp = Q[idx].first;
		idx--;
		checked++;
		
		// Move load to the previous partition.
		if (sp == numPartitions-1 || sp != 0 && curr[sp-1] < curr[sp+1])
		{
			auto gid = sbboundaries[sp];
			auto newSP = (curr_sR[sp]-shistR[gid])*(curr_sS[sp]-shistS[gid]);
			auto newSP1 = (curr_sR[sp-1]+shistR[gid])*(curr_sS[sp-1]+shistS[gid]);
			
			while ((gid != numGranules-1) && (newSP > newSP1))
			{
				curr_sR[sp] -= shistR[gid];
				curr_sS[sp] -= shistS[gid];
				curr[sp] = newSP;
				curr_sR[sp-1] += shistR[gid];
				curr_sS[sp-1] += shistS[gid];
				curr[sp-1] = newSP1;
				boundaries[sp-1] += granuleExtent;
				sbboundaries[sp]++;
				
				gid++;
				newSP = (curr_sR[sp]-shistR[gid])*(curr_sS[sp]-shistS[gid]);
				newSP1 = (curr_sR[sp-1]+shistR[gid])*(curr_sS[sp-1]+shistS[gid]);
				updated = checked;
			}
		}
		// Move load to the next partition.
		else if (sp == 0 || curr[sp-1] > curr[sp+1])
		{
			auto gid = sbboundaries[sp+1]-1;
			auto newSP = (curr_sR[sp]-shistR[gid])*(curr_sS[sp]-shistS[gid]);
			auto newSP1 = (curr_sR[sp+1]+shistR[gid])*(curr_sS[sp+1]+shistS[gid]);
			
			while ((gid != -1) && (newSP > newSP1))
			{
				curr_sR[sp] -= shistR[gid];
				curr_sS[sp] -= shistS[gid];
				curr[sp] = newSP;
				curr_sR[sp+1] += shistR[gid];
				curr_sS[sp+1] += shistS[gid];
				curr[sp+1] = newSP1;
				boundaries[sp] -= granuleExtent;
				sbboundaries[sp+1]--;
				
				gid--;
				newSP = (curr_sR[sp]-shistR[gid])*(curr_sS[sp]-shistS[gid]);
				newSP1 = (curr_sR[sp+1]+shistR[gid])*(curr_sS[sp+1]+shistS[gid]);
				updated = checked;
			}
		}
	}

	free(sbboundaries);
	free(curr);
	free(curr_sR);
	free(curr_sS);
}


////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

struct CountPartitionContentsStructure4
{
	Relation* rel1;
	Relation* rel2;

	uint32_t **pseR_size_local;
	uint32_t **psR_size_local;
	uint32_t **peR_size_local;
	uint32_t **pcR_size_local;

	uint32_t **pseS_size_local;
	uint32_t **psS_size_local;
	uint32_t **peS_size_local;
	uint32_t **pcS_size_local;

	uint64_t *boundaries;
	Timestamp domain_start;
	Timestamp domain_end;
	uint64_t granuleExtent;
	uint32_t numGranules;

	uint32_t thread_id;
	uint32_t numThreads;
};

void* count_partition_contents4(void* args)
{
	CountPartitionContentsStructure4* cpcs = (CountPartitionContentsStructure4*) args;
	uint32_t toTakeStart, toTakeEnd;

	// relation R
	chunk_to_read4( cpcs->rel1->numRecords, cpcs->numThreads, cpcs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t j = toTakeStart-1; j < toTakeEnd; j++)
	{
		auto i = 0;
		while ((i < cpcs->numThreads) && (cpcs->boundaries[i] < (*(cpcs->rel1)).record_list[j].start))
			i++;

		if (i == cpcs->numThreads)
			i = cpcs->numThreads-1;
		auto start_p = i;
		bool check = false;

		i++;
		while ((i < cpcs->numThreads) && (cpcs->boundaries[i-1] < (*(cpcs->rel1)).record_list[j].end))
		{
			if ((cpcs->boundaries[i] < (*(cpcs->rel1)).record_list[j].end) && (i != cpcs->numThreads-1))
			{
				check = true;
				cpcs->pcR_size_local[cpcs->thread_id][i]++;
			}
			else
			{
				check = true;
				cpcs->peR_size_local[cpcs->thread_id][i]++;
			}
			i++;
		}

		if (check)
		{
			cpcs->psR_size_local[cpcs->thread_id][start_p]++;
		}
		else
		{
			cpcs->pseR_size_local[cpcs->thread_id][start_p]++;
		}
	}

	// relation S
	chunk_to_read4( cpcs->rel2->numRecords, cpcs->numThreads, cpcs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t j = toTakeStart-1; j < toTakeEnd; j++)
	{
		auto i = 0;
		while ((i < cpcs->numThreads) && (cpcs->boundaries[i] < (*(cpcs->rel2)).record_list[j].start))
			i++;

		if (i == cpcs->numThreads)
			i = cpcs->numThreads-1;
		auto start_p = i;
		bool check = false;

		i++;
		while ((i < cpcs->numThreads) && (cpcs->boundaries[i-1] < (*(cpcs->rel2)).record_list[j].end))
		{
			if ((cpcs->boundaries[i] < (*(cpcs->rel2)).record_list[j].end) && (i != cpcs->numThreads-1))
			{
				check = true;
				cpcs->pcS_size_local[cpcs->thread_id][i]++;
			}
			else
			{
				check = true;
				cpcs->peS_size_local[cpcs->thread_id][i]++;
			}
			i++;
		}

		if (check)
		{
			cpcs->psS_size_local[cpcs->thread_id][start_p]++;
		}
		else
		{
			cpcs->pseS_size_local[cpcs->thread_id][start_p]++;
		}
	}

	return NULL;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

struct AllocatePartitionsStructure4
{
	Relation *pseR;
	Relation *psR;
	Relation *peR;
	Relation *pcR;
	Relation *pseS;
	Relation *psS;
	Relation *peS;
	Relation *pcS;

	uint32_t *pseR_size;
	uint32_t *psR_size;
	uint32_t *peR_size;
	uint32_t *pcR_size;
	uint32_t *pseS_size;
	uint32_t *psS_size;
	uint32_t *peS_size;
	uint32_t *pcS_size;
	uint32_t **pseR_size_local;
	uint32_t **psR_size_local;
	uint32_t **peR_size_local;
	uint32_t **pcR_size_local;
	uint32_t **pseS_size_local;
	uint32_t **psS_size_local;
	uint32_t **peS_size_local;
	uint32_t **pcS_size_local;

	uint32_t allocationStrategy;
	uint32_t thread_id;
	uint32_t numThreads;
};

void* allocate_partitions4(void* args)
{
	AllocatePartitionsStructure4* aps = (AllocatePartitionsStructure4*) args;

	// count sizes
	for (uint32_t j = 0; j < aps->numThreads; j++)
	{
		aps->pseR_size[aps->thread_id] += aps->pseR_size_local[j][aps->thread_id];
		aps->psR_size[aps->thread_id] += aps->psR_size_local[j][aps->thread_id];
		aps->peR_size[aps->thread_id] += aps->peR_size_local[j][aps->thread_id];
		aps->pcR_size[aps->thread_id] += aps->pcR_size_local[j][aps->thread_id];
		aps->pseS_size[aps->thread_id] += aps->pseS_size_local[j][aps->thread_id];
		aps->psS_size[aps->thread_id] += aps->psS_size_local[j][aps->thread_id];
		aps->peS_size[aps->thread_id] += aps->peS_size_local[j][aps->thread_id];
		aps->pcS_size[aps->thread_id] += aps->pcS_size_local[j][aps->thread_id];
	}

	// allocate R
	aps->pseR[aps->thread_id].record_list = aps->pseR_size[aps->thread_id] ? (Record*) malloc(aps->pseR_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->pseR[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->psR[aps->thread_id].record_list = aps->psR_size[aps->thread_id] ? (Record*) malloc(aps->psR_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->psR[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->peR[aps->thread_id].record_list = aps->peR_size[aps->thread_id] ? (Record*) malloc(aps->peR_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->peR[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->pcR[aps->thread_id].record_list = aps->pcR_size[aps->thread_id] ? (Record*) malloc(aps->pcR_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->pcR[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->pseR[aps->thread_id].numRecords = aps->pseR_size[aps->thread_id];
	aps->pseR[aps->thread_id].minStart = std::numeric_limits<Timestamp>::max();
	aps->pseR[aps->thread_id].maxStart = std::numeric_limits<Timestamp>::min();
	aps->pseR[aps->thread_id].minEnd   = std::numeric_limits<Timestamp>::max();
	aps->pseR[aps->thread_id].maxEnd   = std::numeric_limits<Timestamp>::min();
	aps->psR[aps->thread_id].numRecords = aps->psR_size[aps->thread_id];
	aps->psR[aps->thread_id].minStart = std::numeric_limits<Timestamp>::max();
	aps->psR[aps->thread_id].maxStart = std::numeric_limits<Timestamp>::min();
	aps->psR[aps->thread_id].minEnd   = std::numeric_limits<Timestamp>::max();
	aps->psR[aps->thread_id].maxEnd   = std::numeric_limits<Timestamp>::min();
	aps->peR[aps->thread_id].numRecords = aps->peR_size[aps->thread_id];
	aps->pcR[aps->thread_id].numRecords = aps->pcR_size[aps->thread_id];

	// allocate S
	aps->pseS[aps->thread_id].record_list = aps->pseS_size[aps->thread_id] ? (Record*) malloc(aps->pseS_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->pseS[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->psS[aps->thread_id].record_list = aps->psS_size[aps->thread_id] ? (Record*) malloc(aps->psS_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->psS[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->peS[aps->thread_id].record_list = aps->peS_size[aps->thread_id] ? (Record*) malloc(aps->peS_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->peS[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->pcS[aps->thread_id].record_list = aps->pcS_size[aps->thread_id] ? (Record*) malloc(aps->pcS_size[aps->thread_id]*sizeof(Record)) : NULL;
	aps->pcS[aps->thread_id].allocationStrategy = aps->allocationStrategy;
	aps->pseS[aps->thread_id].numRecords = aps->pseS_size[aps->thread_id];
	aps->pseS[aps->thread_id].minStart = std::numeric_limits<Timestamp>::max();
	aps->pseS[aps->thread_id].maxStart = std::numeric_limits<Timestamp>::min();
	aps->pseS[aps->thread_id].minEnd   = std::numeric_limits<Timestamp>::max();
	aps->pseS[aps->thread_id].maxEnd   = std::numeric_limits<Timestamp>::min();
	aps->psS[aps->thread_id].numRecords = aps->psS_size[aps->thread_id];
	aps->psS[aps->thread_id].minStart = std::numeric_limits<Timestamp>::max();
	aps->psS[aps->thread_id].maxStart = std::numeric_limits<Timestamp>::min();
	aps->psS[aps->thread_id].minEnd   = std::numeric_limits<Timestamp>::max();
	aps->psS[aps->thread_id].maxEnd   = std::numeric_limits<Timestamp>::min();
	aps->peS[aps->thread_id].numRecords = aps->peS_size[aps->thread_id];
	aps->pcS[aps->thread_id].numRecords = aps->pcS_size[aps->thread_id];

	return NULL;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

struct SetPartitionContentsStructure4
{
	Relation* rel1;
	Relation* rel2;

	Relation *pseR;
	Relation *psR;
	Relation *peR;
	Relation *pcR;
	Relation *pseS;
	Relation *psS;
	Relation *peS;
	Relation *pcS;

	uint32_t **pseR_size_local;
	uint32_t **psR_size_local;
	uint32_t **peR_size_local;
	uint32_t **pcR_size_local;
	uint32_t **pseS_size_local;
	uint32_t **psS_size_local;
	uint32_t **peS_size_local;
	uint32_t **pcS_size_local;
	Timestamp **min_max_local_seR;
	Timestamp **min_max_local_sR;
	Timestamp **min_max_local_seS;
	Timestamp **min_max_local_sS;

	uint64_t *boundaries;
	Timestamp domain_start;
	Timestamp domain_end;
	uint64_t granuleExtent;
	uint32_t numGranules;

	uint32_t thread_id;
	uint32_t numThreads;
};

void* set_partition_contents4(void* args)
{
	SetPartitionContentsStructure4* spcs = (SetPartitionContentsStructure4*) args;
	uint32_t toTakeStart, toTakeEnd;
	uint32_t start_p, end_p;

	// relation R
	uint32_t point_to_write_pse[spcs->numThreads];
	uint32_t point_to_write_ps[spcs->numThreads];
	uint32_t point_to_write_pe[spcs->numThreads];
	uint32_t point_to_write_pc[spcs->numThreads];
	for (uint32_t i = 0; i < spcs->numThreads; i++)
	{
		point_to_write_pse[i] = 0;
		point_to_write_ps[i] = 0;
		point_to_write_pe[i] = 0;
		point_to_write_pc[i] = 0;
		for (uint32_t j = 0; j < spcs->thread_id; j++)
		{
			point_to_write_pse[i] += spcs->pseR_size_local[j][i];
			point_to_write_ps[i] += spcs->psR_size_local[j][i];
			point_to_write_pe[i] += spcs->peR_size_local[j][i];
			point_to_write_pc[i] += spcs->pcR_size_local[j][i];
		}
	}

	chunk_to_read4( spcs->rel1->numRecords, spcs->numThreads, spcs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t j = toTakeStart-1; j < toTakeEnd; j++)
	{
		auto i = 0;
		while ((i < spcs->numThreads) && (spcs->boundaries[i] < (*(spcs->rel1)).record_list[j].start))
			i++;

		if (i == spcs->numThreads)
			i = spcs->numThreads-1;
		auto start_p = i;
		bool check = false;

		i++;
		while ((i < spcs->numThreads) && (spcs->boundaries[i-1] < (*(spcs->rel1)).record_list[j].end))
		{
			if ((spcs->boundaries[i] < (*(spcs->rel1)).record_list[j].end) && (i != spcs->numThreads-1))
			{
				spcs->pcR[i].record_list[ point_to_write_pc[i]++ ] = (*(spcs->rel1)).record_list[j];
				check = true;
			}
			else
			{
				spcs->peR[i].record_list[ point_to_write_pe[i]++ ] = (*(spcs->rel1)).record_list[j];
				check = true;
			}
			i++;
		}

		if (check)
		{
			spcs->psR[start_p].record_list[ point_to_write_ps[start_p]++ ] = (*(spcs->rel1)).record_list[j];
			spcs->min_max_local_sR[start_p][0] = std::min(spcs->min_max_local_sR[start_p][0], (*(spcs->rel1)).record_list[j].start);
			spcs->min_max_local_sR[start_p][1] = std::max(spcs->min_max_local_sR[start_p][1], (*(spcs->rel1)).record_list[j].start);
			spcs->min_max_local_sR[start_p][2] = std::min(spcs->min_max_local_sR[start_p][2], (*(spcs->rel1)).record_list[j].end);
			spcs->min_max_local_sR[start_p][3] = std::max(spcs->min_max_local_sR[start_p][3], (*(spcs->rel1)).record_list[j].end);
		}
		else
		{
			spcs->pseR[start_p].record_list[ point_to_write_pse[start_p]++ ] = (*(spcs->rel1)).record_list[j];
			spcs->min_max_local_seR[start_p][0] = std::min(spcs->min_max_local_seR[start_p][0], (*(spcs->rel1)).record_list[j].start);
			spcs->min_max_local_seR[start_p][1] = std::max(spcs->min_max_local_seR[start_p][1], (*(spcs->rel1)).record_list[j].start);
			spcs->min_max_local_seR[start_p][2] = std::min(spcs->min_max_local_seR[start_p][2], (*(spcs->rel1)).record_list[j].end);
			spcs->min_max_local_seR[start_p][3] = std::max(spcs->min_max_local_seR[start_p][3], (*(spcs->rel1)).record_list[j].end);
		}
	}

	// relation S
	for (uint32_t i = 0; i < spcs->numThreads; i++)
	{
		point_to_write_pse[i] = 0;
		point_to_write_ps[i] = 0;
		point_to_write_pe[i] = 0;
		point_to_write_pc[i] = 0;
		for (uint32_t j = 0; j < spcs->thread_id; j++)
		{
			point_to_write_pse[i] += spcs->pseS_size_local[j][i];
			point_to_write_ps[i] += spcs->psS_size_local[j][i];
			point_to_write_pe[i] += spcs->peS_size_local[j][i];
			point_to_write_pc[i] += spcs->pcS_size_local[j][i];
		}
	}

	chunk_to_read4( spcs->rel2->numRecords, spcs->numThreads, spcs->thread_id, toTakeStart, toTakeEnd);
	for (uint32_t j = toTakeStart-1; j < toTakeEnd; j++)
	{
		auto i = 0;
		while ((i < spcs->numThreads) && (spcs->boundaries[i] < (*(spcs->rel2)).record_list[j].start))
			i++;

		if (i == spcs->numThreads)
			i = spcs->numThreads-1;
		auto start_p = i;
		bool check = false;

		i++;
		while ((i < spcs->numThreads) && (spcs->boundaries[i-1] < (*(spcs->rel2)).record_list[j].end))
		{
			if ((spcs->boundaries[i] < (*(spcs->rel2)).record_list[j].end) && (i != spcs->numThreads-1))
			{
				check = true;
				spcs->pcS[i].record_list[ point_to_write_pc[i]++ ] = (*(spcs->rel2)).record_list[j];
			}
			else
			{
				check = true;
				spcs->peS[i].record_list[ point_to_write_pe[i]++ ] = (*(spcs->rel2)).record_list[j];
			}
			i++;
		}

		if (check)
		{
			spcs->psS[start_p].record_list[ point_to_write_ps[start_p]++ ] = (*(spcs->rel2)).record_list[j];
			spcs->min_max_local_sS[start_p][0] = std::min(spcs->min_max_local_sS[start_p][0], (*(spcs->rel2)).record_list[j].start);
			spcs->min_max_local_sS[start_p][1] = std::max(spcs->min_max_local_sS[start_p][1], (*(spcs->rel2)).record_list[j].start);
			spcs->min_max_local_sS[start_p][2] = std::min(spcs->min_max_local_sS[start_p][2], (*(spcs->rel2)).record_list[j].end);
			spcs->min_max_local_sS[start_p][3] = std::max(spcs->min_max_local_sS[start_p][3], (*(spcs->rel2)).record_list[j].end);
		}
		else
		{
			spcs->pseS[start_p].record_list[ point_to_write_pse[start_p]++ ] = (*(spcs->rel2)).record_list[j];
			spcs->min_max_local_seS[start_p][0] = std::min(spcs->min_max_local_seS[start_p][0], (*(spcs->rel2)).record_list[j].start);
			spcs->min_max_local_seS[start_p][1] = std::max(spcs->min_max_local_seS[start_p][1], (*(spcs->rel2)).record_list[j].start);
			spcs->min_max_local_seS[start_p][2] = std::min(spcs->min_max_local_seS[start_p][2], (*(spcs->rel2)).record_list[j].end);
			spcs->min_max_local_seS[start_p][3] = std::max(spcs->min_max_local_seS[start_p][3], (*(spcs->rel2)).record_list[j].end);
		}
	}

	return NULL;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

void SetBoundaries4(Relation& R, Relation& S,
					Timestamp domain_start, Timestamp domain_end, uint32_t numGranules, Timestamp granuleExtent,
					uint64_t *boundaries, uint32_t numThreads)
{
	pthread_t thread_id[numThreads];

	// Step 1a: build histogram statistics for granules; i.e., count the records starting inside each granule
	Timer tim;
	tim.start();

	// Use all available parallel threads; each thread reads an equal-sized chunk of both relations and keeps local histogam information
	BuildStartHistogramsStructure4 bshs[numThreads];
	uint32_t** local_shistR = (uint32_t**) malloc( numThreads * sizeof(uint32_t*) );
	uint32_t** local_shistS = (uint32_t**) malloc( numThreads * sizeof(uint32_t*) );
	for (uint32_t i = 0; i < numThreads; i++)
	{
		local_shistR[i] = (uint32_t*) malloc( numGranules * sizeof(uint32_t) );
		local_shistS[i] = (uint32_t*) malloc( numGranules * sizeof(uint32_t) );
		for (uint32_t j = 0; j < numGranules; j++)
		{
			local_shistR[i][j] = 0;
			local_shistS[i][j] = 0;
		}
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		bshs[i].relR = &R;
		bshs[i].relS = &S;
		bshs[i].domain_start = domain_start;
		bshs[i].domain_end = domain_end;
		bshs[i].granuleExtent = granuleExtent;
		bshs[i].numGranules = numGranules;
		bshs[i].shistR = local_shistR[i];
		bshs[i].shistS = local_shistS[i];
		bshs[i].thread_id = i;
		bshs[i].numThreads = numThreads;
		pthread_create( &thread_id[i], NULL, BuildStartHistograms4, (void*) &bshs[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	// sum local histograms to global ones
	uint32_t *shistR = (uint32_t*) malloc(numGranules * sizeof(uint32_t));
	uint32_t *shistS = (uint32_t*) malloc(numGranules * sizeof(uint32_t));
	for (uint32_t i = 0; i < numGranules; i++)
	{
		shistR[i] = 0;
		shistS[i] = 0;
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		for (uint32_t j = 0; j < numGranules; j++)
		{
			shistR[j] += local_shistR[i][j];
			shistS[j] += local_shistS[i][j];
		}
		free(local_shistR[i]);
		free(local_shistS[i]);
	}
	free(local_shistR);
	free(local_shistS);

	double HistosTime = tim.stop();
	printf("Histos:                     %lf secs\n", HistosTime);

	// Step 1b: initialize and reposition the boundaries between partitions;
	// Use one thread.
	tim.start();

	AdaptivePartitioning4(shistR, shistS, boundaries, domain_end, granuleExtent, numGranules, numThreads);

	free(shistR);
	free(shistS);
/*
for (uint32_t i = 0; i < numGranules; i++)
{
	std::cout << shistR[i] << " " << shistS[i] << std::endl;
}
std::cout << "total " << numGranules << std::endl;
exit(1);
*/
	double AdaptiveTime = tim.stop();
	printf("Adaptive:                   %lf secs\n", AdaptiveTime);
/*
for (uint32_t i = 0; i < numThreads; i++)
{
	std::cout << boundaries[i] << std::endl;
}
exit(1);
*/
	return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

void ParallelPartitioning4( Relation& R, Relation& S, Relation *pseR, Relation *pseS, Relation *psR, Relation *psS, Relation *pcR, Relation *pcS, Relation *peR, Relation *peS, uint64_t *boundaries, uint64_t granuleExtent, uint32_t numGranules, Timestamp domain_start, Timestamp domain_end, uint32_t numThreads, int allocationStrategy)
{
	Timer tim;
	tim.start();

	// Initiliaze global counters
	uint32_t *pseR_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *psR_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *peR_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *pcR_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *pseS_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *psS_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *peS_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	uint32_t *pcS_size = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
	for (uint32_t j = 0; j < numThreads; j++)
	{
		pseR_size[j] = 0;
		psR_size[j] = 0;
		peR_size[j] = 0;
		pcR_size[j] = 0;
		pseS_size[j] = 0;
		psS_size[j] = 0;
		peS_size[j] = 0;
		pcS_size[j] = 0;
	}
	// Initialize  local counters
	uint32_t **pseR_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **psR_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **peR_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **pcR_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **pseS_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **psS_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **peS_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	uint32_t **pcS_size_local = (uint32_t**) malloc(numThreads * sizeof(uint32_t*));
	for (uint32_t j = 0; j < numThreads; j++)
	{
		pseR_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		psR_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		peR_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		pcR_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		pseS_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		psS_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		peS_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		pcS_size_local[j] = (uint32_t*) malloc(numThreads * sizeof(uint32_t));
		for (uint32_t i = 0; i < numThreads; i++)
		{
			pseR_size_local[j][i] = 0;
			psR_size_local[j][i] = 0;
			peR_size_local[j][i] = 0;
			pcR_size_local[j][i] = 0;
			pseS_size_local[j][i] = 0;
			psS_size_local[j][i] = 0;
			peS_size_local[j][i] = 0;
			pcS_size_local[j][i] = 0;
		}
	}

	// Step2a: Count partition contents
	pthread_t thread_id[numThreads];
	CountPartitionContentsStructure4 cpcs[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		cpcs[i].rel1 = &R;
		cpcs[i].rel2 = &S;
		cpcs[i].pseR_size_local = pseR_size_local;
		cpcs[i].psR_size_local = psR_size_local;
		cpcs[i].peR_size_local = peR_size_local;
		cpcs[i].pcR_size_local = pcR_size_local;
		cpcs[i].pseS_size_local = pseS_size_local;
		cpcs[i].psS_size_local = psS_size_local;
		cpcs[i].peS_size_local = peS_size_local;
		cpcs[i].pcS_size_local = pcS_size_local;
		cpcs[i].boundaries = boundaries;
		cpcs[i].domain_start = domain_start;
		cpcs[i].domain_end = domain_end;
		cpcs[i].granuleExtent = granuleExtent;
		cpcs[i].numGranules = numGranules;
		cpcs[i].thread_id = i;
		cpcs[i].numThreads = numThreads;
		pthread_create( &thread_id[i], NULL, count_partition_contents4, (void*) &cpcs[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	// Step2b: Allocate memory space for partitions
	AllocatePartitionsStructure4 aps[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		aps[i].pseR = pseR;
		aps[i].psR = psR;
		aps[i].peR = peR;
		aps[i].pcR = pcR;
		aps[i].pseS = pseS;
		aps[i].psS = psS;
		aps[i].peS = peS;
		aps[i].pcS = pcS;
		aps[i].pseR_size = pseR_size;
		aps[i].psR_size = psR_size;
		aps[i].peR_size = peR_size;
		aps[i].pcR_size = pcR_size;
		aps[i].pseS_size = pseS_size;
		aps[i].psS_size = psS_size;
		aps[i].peS_size = peS_size;
		aps[i].pcS_size = pcS_size;
		aps[i].pseR_size_local = pseR_size_local;
		aps[i].psR_size_local = psR_size_local;
		aps[i].peR_size_local = peR_size_local;
		aps[i].pcR_size_local = pcR_size_local;
		aps[i].pseS_size_local = pseS_size_local;
		aps[i].psS_size_local = psS_size_local;
		aps[i].peS_size_local = peS_size_local;
		aps[i].pcS_size_local = pcS_size_local;
		aps[i].allocationStrategy = allocationStrategy;
		aps[i].thread_id = i;
		aps[i].numThreads = numThreads;
		pthread_create( &thread_id[i], NULL, allocate_partitions4, (void*) &aps[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	// Step2c: Set partition contents
	SetPartitionContentsStructure4 spcs[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		spcs[i].rel1 = &R;
		spcs[i].rel2 = &S;
		spcs[i].pseR = pseR;
		spcs[i].psR = psR;
		spcs[i].peR = peR;
		spcs[i].pcR = pcR;
		spcs[i].pseS = pseS;
		spcs[i].psS = psS;
		spcs[i].peS = peS;
		spcs[i].pcS = pcS;
		spcs[i].pseR_size_local = pseR_size_local;
		spcs[i].psR_size_local = psR_size_local;
		spcs[i].peR_size_local = peR_size_local;
		spcs[i].pcR_size_local = pcR_size_local;
		spcs[i].pseS_size_local = pseS_size_local;
		spcs[i].psS_size_local = psS_size_local;
		spcs[i].peS_size_local = peS_size_local;
		spcs[i].pcS_size_local = pcS_size_local;
		spcs[i].boundaries = boundaries;
		spcs[i].domain_start = domain_start;
		spcs[i].domain_end = domain_end;
		spcs[i].granuleExtent = granuleExtent;
		spcs[i].numGranules = numGranules;
		spcs[i].min_max_local_seR = (Timestamp**) malloc(numThreads * sizeof(Timestamp*) );
		spcs[i].min_max_local_sR = (Timestamp**) malloc(numThreads * sizeof(Timestamp*) );
		spcs[i].min_max_local_seS = (Timestamp**) malloc(numThreads * sizeof(Timestamp*) );
		spcs[i].min_max_local_sS = (Timestamp**) malloc(numThreads * sizeof(Timestamp*) );
		for (uint32_t j = 0; j < numThreads; j++)
		{
			spcs[i].min_max_local_seR[j] = (Timestamp*) malloc(4 * sizeof(Timestamp));
			spcs[i].min_max_local_seR[j][0] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_seR[j][1] = std::numeric_limits<Timestamp>::min();
			spcs[i].min_max_local_seR[j][2] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_seR[j][3] = std::numeric_limits<Timestamp>::min();

			spcs[i].min_max_local_sR[j] = (Timestamp*) malloc(4 * sizeof(Timestamp));
			spcs[i].min_max_local_sR[j][0] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_sR[j][1] = std::numeric_limits<Timestamp>::min();
			spcs[i].min_max_local_sR[j][2] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_sR[j][3] = std::numeric_limits<Timestamp>::min();

			spcs[i].min_max_local_seS[j] = (Timestamp*) malloc(4 * sizeof(Timestamp));
			spcs[i].min_max_local_seS[j][0] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_seS[j][1] = std::numeric_limits<Timestamp>::min();
			spcs[i].min_max_local_seS[j][2] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_seS[j][3] = std::numeric_limits<Timestamp>::min();

			spcs[i].min_max_local_sS[j] = (Timestamp*) malloc(4 * sizeof(Timestamp));
			spcs[i].min_max_local_sS[j][0] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_sS[j][1] = std::numeric_limits<Timestamp>::min();
			spcs[i].min_max_local_sS[j][2] = std::numeric_limits<Timestamp>::max();
			spcs[i].min_max_local_sS[j][3] = std::numeric_limits<Timestamp>::min();
		}
		spcs[i].thread_id = i;
		spcs[i].numThreads = numThreads;
		pthread_create( &thread_id[i], NULL, set_partition_contents4, (void*) &spcs[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}
	for (uint32_t j = 0; j < numThreads; j++)
	{
		for (uint32_t i = 0; i < numThreads; i++)
		{
			pseR[j].minStart = std::min( pseR[j].minStart, spcs[i].min_max_local_seR[j][0]);
			pseR[j].maxStart = std::max( pseR[j].maxStart, spcs[i].min_max_local_seR[j][1]);
			pseR[j].minEnd   = std::min( pseR[j].minEnd,   spcs[i].min_max_local_seR[j][2]);
			pseR[j].maxEnd   = std::max( pseR[j].maxEnd,   spcs[i].min_max_local_seR[j][3]);
			free(spcs[i].min_max_local_seR[j]);

			psR[j].minStart = std::min( psR[j].minStart, spcs[i].min_max_local_sR[j][0]);
			psR[j].maxStart = std::max( psR[j].maxStart, spcs[i].min_max_local_sR[j][1]);
			psR[j].minEnd   = std::min( psR[j].minEnd,   spcs[i].min_max_local_sR[j][2]);
			psR[j].maxEnd   = std::max( psR[j].maxEnd,   spcs[i].min_max_local_sR[j][3]);
			free(spcs[i].min_max_local_sR[j]);

			pseS[j].minStart = std::min( pseS[j].minStart, spcs[i].min_max_local_seS[j][0]);
			pseS[j].maxStart = std::max( pseS[j].maxStart, spcs[i].min_max_local_seS[j][1]);
			pseS[j].minEnd   = std::min( pseS[j].minEnd,   spcs[i].min_max_local_seS[j][2]);
			pseS[j].maxEnd   = std::max( pseS[j].maxEnd,   spcs[i].min_max_local_seS[j][3]);
			free(spcs[i].min_max_local_seS[j]);

			psS[j].minStart = std::min( psS[j].minStart, spcs[i].min_max_local_sS[j][0]);
			psS[j].maxStart = std::max( psS[j].maxStart, spcs[i].min_max_local_sS[j][1]);
			psS[j].minEnd   = std::min( psS[j].minEnd,   spcs[i].min_max_local_sS[j][2]);
			psS[j].maxEnd   = std::max( psS[j].maxEnd,   spcs[i].min_max_local_sS[j][3]);
			free(spcs[i].min_max_local_sS[j]);
		}
	}

	// free memory space
	for (uint32_t j = 0; j < numThreads; j++)
	{
		free(spcs[j].min_max_local_seR);
		free(spcs[j].min_max_local_sR);
		free(spcs[j].min_max_local_seS);
		free(spcs[j].min_max_local_sS);

		free(pseR_size_local[j]);
		free(psR_size_local[j]);
		free(peR_size_local[j]);
		free(pcR_size_local[j]);
		free(pseS_size_local[j]);
		free(psS_size_local[j]);
		free(peS_size_local[j]);
		free(pcS_size_local[j]);
	}
	free(pseR_size_local);
	free(psR_size_local);
	free(peR_size_local);
	free(pcR_size_local);
	free(pseS_size_local);
	free(psS_size_local);
	free(peS_size_local);
	free(pcS_size_local);

	free(pseR_size);
	free(psR_size);
	free(peR_size);
	free(pcR_size);
	free(pseS_size);
	free(psS_size);
	free(peS_size);
	free(pcS_size);

	double SettingTime = tim.stop();
	printf("Setting:                    %lf secs\n", SettingTime);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

struct SortPartitionsStructure4
{
	Relation *seR;
	Relation *sR;
	Relation *seS;
	Relation *sS;
	Relation *eR;
	Relation *eS;

	uint32_t thread_id;
};

void* sort_partitions4(void* args)
{
	SortPartitionsStructure4* sps = (SortPartitionsStructure4*) args;

	oneapi::tbb::task_arena arenaE(1);
	oneapi::tbb::task_arena arenaF(1);
	oneapi::tbb::task_arena arenaA(1);
	oneapi::tbb::task_arena arenaB(1);
	oneapi::tbb::task_arena arenaC(1);
	oneapi::tbb::task_arena arenaD(1);
	arenaE.execute([&]{ oneapi::tbb::parallel_sort( &sps->seR->record_list[0], &sps->seR->record_list[0] + sps->seR->numRecords, CompareByStart{} ); });
	arenaF.execute([&]{ oneapi::tbb::parallel_sort( &sps->seS->record_list[0], &sps->seS->record_list[0] + sps->seS->numRecords, CompareByStart{} ); });
	arenaA.execute([&]{ oneapi::tbb::parallel_sort( &sps->sR->record_list[0], &sps->sR->record_list[0] + sps->sR->numRecords, CompareByStart{} ); });
	arenaB.execute([&]{ oneapi::tbb::parallel_sort( &sps->sS->record_list[0], &sps->sS->record_list[0] + sps->sS->numRecords, CompareByStart{} ); });
	arenaC.execute([&]{ oneapi::tbb::parallel_sort( &sps->eR->record_list[0], &sps->eR->record_list[0] + sps->eR->numRecords, CompareByEnd{} ); });
	arenaD.execute([&]{ oneapi::tbb::parallel_sort( &sps->eS->record_list[0], &sps->eS->record_list[0] + sps->eS->numRecords, CompareByEnd{} ); });
//	std::sort( &sps->seR->record_list[0], &sps->seR->record_list[0] + sps->seR->numRecords, CompareByStart{} );
//	std::sort( &sps->seS->record_list[0], &sps->seS->record_list[0] + sps->seS->numRecords, CompareByStart{} );
//	std::sort( &sps->sR->record_list[0], &sps->sR->record_list[0] + sps->sR->numRecords, CompareByStart{} );
//	std::sort( &sps->sS->record_list[0], &sps->sS->record_list[0] + sps->sS->numRecords, CompareByStart{} );
//	std::sort( &sps->eR->record_list[0], &sps->eR->record_list[0] + sps->eR->numRecords, CompareByEnd{} );
//	std::sort( &sps->eS->record_list[0], &sps->eS->record_list[0] + sps->eS->numRecords, CompareByEnd{} );

	return NULL;
}

struct SwitchToColumnStoreStructure4
{
	Relation* seR;
	Relation* sR;
	Relation* seS;
	Relation* sS;
	Relation* cR;
	Relation* cS;
	Relation* eR;
	Relation* eS;

	uint32_t thread_id;
	uint32_t core_id;
};

void *switch_to_column_store4(void* args)
{
	SwitchToColumnStoreStructure4* stcss = (SwitchToColumnStoreStructure4*) args;

	stcss->seR->columnStoreSwitch( stcss->core_id );
	stcss->seS->columnStoreSwitch( stcss->core_id );
	stcss->sR->columnStoreSwitch( stcss->core_id );
	stcss->sS->columnStoreSwitch( stcss->core_id );
	stcss->cR->columnStoreSwitch( stcss->core_id );
	stcss->cS->columnStoreSwitch( stcss->core_id );
	stcss->eR->columnStoreSwitch( stcss->core_id );
	stcss->eS->columnStoreSwitch( stcss->core_id );

	return NULL;
}

struct BuildBucketsStructure4
{
	Relation *seR;
	Relation *seS;
	BucketIndex *seBIR;
	BucketIndex *seBIS;

	Relation *sR;
	Relation *sS;
	BucketIndex *sBIR;
	BucketIndex *sBIS;

	uint32_t thread_id;
};

void* build_buckets4(void* args)
{
	BuildBucketsStructure4* bbs = (BuildBucketsStructure4*) args;

	bbs->seBIR->build( *(bbs->seR), 1000);
	bbs->seBIS->build( *(bbs->seS), 1000);

	bbs->sBIR->build( *(bbs->sR), 1000);
	bbs->sBIS->build( *(bbs->sS), 1000);

	return NULL;
}

uint32_t Parallel_Sorting_BucketBuilding4(Relation *pseR, Relation *pseS, Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS, BucketIndex *pseBIR, BucketIndex *pseBIS, BucketIndex *psBIR, BucketIndex *psBIS, uint32_t numThreads, int* thread_core_map)
{
	// execute minijoins with greedy sceduling
	Timer tim;

	// sort relations
	tim.start();
	pthread_t thread_id[numThreads];
	SortPartitionsStructure4 sps[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		sps[i].seR = &pseR[i];
		sps[i].sR = &psR[i];
		sps[i].seS = &pseS[i];
		sps[i].sS = &psS[i];
		sps[i].eR = &peR[i];
		sps[i].eS = &peS[i];

		sps[i].thread_id = i;

		pthread_create( &thread_id[i], NULL, sort_partitions4, (void*) &sps[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}
	double timeSorting = tim.stop();
	printf("Sorting:                    %lf secs\n", timeSorting);

	// switch to column store
	tim.start();

	SwitchToColumnStoreStructure4 stcss[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		stcss[i].seR = &pseR[i];
		stcss[i].sR = &psR[i];
		stcss[i].seS = &pseS[i];
		stcss[i].sS = &psS[i];
		stcss[i].cR = &pcR[i];
		stcss[i].cS = &pcS[i];
		stcss[i].eR = &peR[i];
		stcss[i].eS = &peS[i];

		stcss[i].thread_id = i;
		stcss[i].core_id   = thread_core_map[i];

		pthread_create( &thread_id[i], NULL, switch_to_column_store4, (void*) &stcss[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}

	double switchingTime = tim.stop();
	printf("Switching to Column Store:  %lf secs\n", switchingTime);

	// selectivity check
	tim.start();
	uint64_t FS_est = FS_estimation( psR[0], psS[0]);
	double selectivityEstimationTime = tim.stop();
	printf("Selectivity Estimation:     %lf secs ", selectivityEstimationTime);

	if (FS_est >= OPT_FS_BARRIER)
		printf("* calculated %ld, going for bgudFS *\n", FS_est);
	else
		printf("* calculated %ld, going for udFS *\n", FS_est);

	// build buckets (always needed, for (E,S) and (S,E) )
	tim.start();
	BuildBucketsStructure4 bbs[numThreads];
	for (uint32_t i = 0; i < numThreads; i++)
	{
		bbs[i].seR = &pseR[i];
		bbs[i].seS = &pseS[i];
		bbs[i].seBIR = &pseBIR[i];
		bbs[i].seBIS = &pseBIS[i];

		bbs[i].sR = &psR[i];
		bbs[i].sS = &psS[i];
		bbs[i].sBIR = &psBIR[i];
		bbs[i].sBIS = &psBIS[i];

		bbs[i].thread_id = i;

		pthread_create( &thread_id[i], NULL, build_buckets4, (void*) &bbs[i]);
	}
	for (uint32_t i = 0; i < numThreads; i++)
	{
		pthread_join( thread_id[i], NULL);
	}
	double timeBucketBuilding = tim.stop();
	printf("Bucketing:                  %lf secs\n", timeBucketBuilding);

	return FS_est;
}

uint64_t create_partitions4( Relation& R, Relation& S, Relation *pseR, Relation *pseS, Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS, BucketIndex *pseBIR, BucketIndex *pseBIS, BucketIndex *psBIR, BucketIndex *psBIS, Timestamp domain_start, Timestamp domain_end, uint32_t numGranules, uint64_t granuleExtent, int numThreads, int allocationStrategy, int* thread_core_map)
{
	Timer tim;
	tim.start();

	// Step 1: Build Histograms and perform adaptive partitioning on starting boundaries.
	uint64_t *boundaries = (uint64_t*) malloc(numThreads*sizeof(uint64_t));
	SetBoundaries4( R, S, domain_start, domain_end, numGranules, granuleExtent, boundaries, numThreads);

	// Step 2: create and fill structures for each partition.
	ParallelPartitioning4( R, S, pseR, pseS, psR, psS, pcR, pcS, peR, peS, boundaries, granuleExtent, numGranules, domain_start, domain_end, numThreads, allocationStrategy);

	// Step 3: sort and build buckets (if needed) for each partition
	uint64_t FS_est = Parallel_Sorting_BucketBuilding4( pseR, pseS, psR, psS, peR, peS, pcR, pcS, pseBIR, pseBIS, psBIR, psBIS, numThreads, thread_core_map);
	free(boundaries);

	double preJoinTime = tim.stop();
	printf(" Pre-Join Time:             %lf secs\n", preJoinTime);

	return FS_est;
}
