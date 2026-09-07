#include "../../def.hpp"
#include "../../containers/relation.hpp"
#include "../../containers/bucket_index.hpp"

uint32_t create_partitions4( Relation&, Relation&, Relation *pseR, Relation *pseS, Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS, BucketIndex *pseBIR, BucketIndex *pseBIS, BucketIndex *psBIR, BucketIndex *psBIS, Timestamp domain_start, Timestamp domain_end, uint32_t numGranules, uint64_t granuleExtent, int numThreads, int allocationStrategy, int* thread_core_map);

uint64_t udFS_wc       (Relation &R, Relation &S);
uint64_t udFS_xor      (Relation &R, Relation &S);
uint64_t udFS_simd_xor (Relation &R, Relation &S);

uint64_t bgudFS_wc       (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_xor      (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);
uint64_t bgudFS_simd_xor (Relation &R, Relation &S, const int max_group_size, const BucketIndex &BIR, const BucketIndex &BIS);

uint64_t bgudFS_internalLoop_wc       ( Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_xor      ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);
uint64_t bgudFS_internalLoop_simd_xor ( Timestamp** pointers_group_start, Timestamp** pointers_group_end, uint32_t group_size, Timestamp* firstFS_start, Timestamp* lastFS, const BucketIndex &BI, Timestamp minStart);

uint64_t bgudFS_Ends_wc(Relation& R, Relation& S, int max_group_size, BucketIndex& BI);
uint64_t bgudFS_Ends_xor(Relation& R, Relation& S, int max_group_size, BucketIndex& BI);
uint64_t bgudFS_Ends_simd_xor(Relation& R, Relation& S, int max_group_size, BucketIndex& BI);

uint64_t NestedLoop_wc(Relation &R, Relation &S);
uint64_t NestedLoop_xor(Relation &R, Relation &S);
uint64_t NestedLoop_simd_xor(Relation &R, Relation &S);

int stick_this_thread_to_core(int core_id);

struct MiniJoinsStructureGreedy4
{
	Relation *pseR;
	Relation *pseS;
	Relation *psR;
	Relation *psS;
	Relation *peR;
	Relation *peS;
	Relation *pcR;
	Relation *pcS;
	int max_group_size;
	BucketIndex *pseBIR;
	BucketIndex *pseBIS;
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

void* run_mini_joins_greedy4(void* args)
{
	MiniJoinsStructureGreedy4* mjsg = (MiniJoinsStructureGreedy4*) args;

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
						res += bgudFS_wc( mjsg->pseR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_wc( mjsg->pseR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 1:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_wc( mjsg->pseR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_wc( mjsg->pseR[mj.first], mjsg->psS[mj.first]);
					break;
				case 2:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_wc( mjsg->psR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_wc( mjsg->psR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 3:
					res += bgudFS_Ends_wc( mjsg->peR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIS[mj.first]);
					break;
				case 4:
					res += bgudFS_Ends_wc( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 5:
					res += bgudFS_Ends_wc( mjsg->peS[mj.first], mjsg->pseR[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first]);
					break;
				case 6:
					res += bgudFS_Ends_wc( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 7:
					res += NestedLoop_wc( mjsg->psR[mj.first], mjsg->psS[mj.first] );
					break;
				case 8:
					res += NestedLoop_wc( mjsg->pseR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 9:
					res += NestedLoop_wc( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 10:
					res += NestedLoop_wc( mjsg->pseS[mj.first], mjsg->pcR[mj.first] );
					break;
				case 11:
					res += NestedLoop_wc( mjsg->psS[mj.first] , mjsg->pcR[mj.first] );
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
						res += bgudFS_xor( mjsg->pseR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_xor( mjsg->pseR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 1:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_xor( mjsg->pseR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_xor( mjsg->pseR[mj.first], mjsg->psS[mj.first]);
					break;
				case 2:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_xor( mjsg->psR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_xor( mjsg->psR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 3:
					res += bgudFS_Ends_xor( mjsg->peR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIS[mj.first]);
					break;
				case 4:
					res += bgudFS_Ends_xor( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 5:
					res += bgudFS_Ends_xor( mjsg->peS[mj.first], mjsg->pseR[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first]);
					break;
				case 6:
					res += bgudFS_Ends_xor( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 7:
					res += NestedLoop_xor( mjsg->psR[mj.first], mjsg->psS[mj.first] );
					break;
				case 8:
					res += NestedLoop_xor( mjsg->pseR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 9:
					res += NestedLoop_xor( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 10:
					res += NestedLoop_xor( mjsg->pseS[mj.first], mjsg->pcR[mj.first] );
					break;
				case 11:
					res += NestedLoop_xor( mjsg->psS[mj.first] , mjsg->pcR[mj.first] );
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
						res += bgudFS_simd_xor( mjsg->pseR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_simd_xor( mjsg->pseR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 1:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_simd_xor( mjsg->pseR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first], mjsg->psBIS[mj.first] );
					else
						res += udFS_simd_xor( mjsg->pseR[mj.first], mjsg->psS[mj.first]);
					break;
				case 2:
					if (mjsg->FS_est >= OPT_FS_BARRIER)
						res += bgudFS_simd_xor( mjsg->psR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first], mjsg->pseBIS[mj.first] );
					else
						res += udFS_simd_xor( mjsg->psR[mj.first], mjsg->pseS[mj.first]);
					break;
				case 3:
					res += bgudFS_Ends_simd_xor( mjsg->peR[mj.first], mjsg->pseS[mj.first], mjsg->max_group_size, mjsg->pseBIS[mj.first]);
					break;
				case 4:
					res += bgudFS_Ends_simd_xor( mjsg->peR[mj.first], mjsg->psS[mj.first], mjsg->max_group_size, mjsg->psBIS[mj.first]);
					break;
				case 5:
					res += bgudFS_Ends_simd_xor( mjsg->peS[mj.first], mjsg->pseR[mj.first], mjsg->max_group_size, mjsg->pseBIR[mj.first]);
					break;
				case 6:
					res += bgudFS_Ends_simd_xor( mjsg->peS[mj.first], mjsg->psR[mj.first], mjsg->max_group_size, mjsg->psBIR[mj.first]);
					break;
				case 7:
					res += NestedLoop_simd_xor( mjsg->psR[mj.first], mjsg->psS[mj.first] );
					break;
				case 8:
					res += NestedLoop_simd_xor( mjsg->pseR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 9:
					res += NestedLoop_simd_xor( mjsg->psR[mj.first], mjsg->pcS[mj.first] );
					break;
				case 10:
					res += NestedLoop_simd_xor( mjsg->pseS[mj.first], mjsg->pcR[mj.first] );
					break;
				case 11:
					res += NestedLoop_simd_xor( mjsg->psS[mj.first] , mjsg->pcR[mj.first] );
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

void greedy_schedule4( Relation* pseR, Relation* psR, Relation* peR, Relation* pcR, Relation* pseS, Relation* psS, Relation* peS, Relation* pcS,
	std::vector< std::pair< std::vector< std::pair<uint32_t,uint32_t> >, uint64_t > > &mini_joins, uint32_t numThreads)
{
	// vector of pair< pair<partition_id,join_type>, load > to store all jobs sorted by load
	std::vector < std::pair< std::pair<uint32_t,uint32_t>, uint64_t> > jobs_list;
	for (uint32_t i = 0; i < numThreads; i++)
	{
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 0), pseR[i].numRecords * (uint64_t) pseS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 1), pseR[i].numRecords * (uint64_t) psS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 2), psR[i].numRecords * (uint64_t) pseS[i].numRecords ) );

		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 3), peR[i].numRecords * (uint64_t) pseS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 4), peR[i].numRecords * (uint64_t) psS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 5), peS[i].numRecords * (uint64_t) pseR[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 6), peS[i].numRecords * (uint64_t) psR[i].numRecords ) );

		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 7), psR[i].numRecords * (uint64_t) psS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 8), pseR[i].numRecords * (uint64_t) pcS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 9), psR[i].numRecords * (uint64_t) pcS[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 10), pseS[i].numRecords * (uint64_t) pcR[i].numRecords ) );
		jobs_list.push_back( std::pair< std::pair<uint32_t,uint32_t>, uint64_t>( std::pair<uint32_t,uint32_t>(i, 11), psS[i].numRecords * (uint64_t) pcR[i].numRecords ) );
	}
	std::sort(jobs_list.begin(), jobs_list.end(), [](const std::pair< std::pair<uint32_t,uint32_t>, uint64_t>& a, const std::pair< std::pair<uint32_t,uint32_t>, uint64_t>& b) { return a.second > b.second; } );

	// initialize thread bags with 0
	for (uint32_t i = 0; i < numThreads; i++)
	{
		mini_joins[i].second = 0;
	}
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
uint64_t ParallelDomainBased_Execute4(Relation *pseR, Relation *pseS, Relation *psR, Relation *psS, Relation *peR, Relation *peS, Relation *pcR, Relation *pcS, int max_group_size, BucketIndex *pseBIR, BucketIndex *pseBIS, BucketIndex *psBIR, BucketIndex *psBIS, uint32_t numThreads, int mode, uint64_t FS_est, int allocationStrategy, int* thread_core_map)
{
	// execute
	// ! we also provide atomic execution beq of NUMA awareness (greedy mixes partitions, so it needs |sockets|) !
	uint64_t result = 0;
	pthread_t thread_id[numThreads];
	uint64_t thread_results[numThreads];
	bool atomicExecution = false;

	if (atomicExecution)
	{
		exit(1);
	}
	else
	{
		// array of pair < vector<pair<partitionId,type>> , cost >
		std::vector< std::pair < std::vector< std::pair<uint32_t,uint32_t> >, uint64_t > > mini_joins(numThreads);
		greedy_schedule4( pseR, psR, peR, pcR, pseS, psS, peS, pcS, mini_joins, numThreads);

		MiniJoinsStructureGreedy4 mjsg[numThreads];
		for (uint32_t i = 0; i < numThreads; i++)
		{
			mjsg[i].pseR = pseR;
			mjsg[i].psR = psR;
			mjsg[i].pseS = pseS;
			mjsg[i].psS = psS;
			mjsg[i].peR = peR;
			mjsg[i].peS = peS;
			mjsg[i].pcR = pcR;
			mjsg[i].pcS = pcS;
			mjsg[i].max_group_size = max_group_size;
			mjsg[i].pseBIR = pseBIR;
			mjsg[i].pseBIS = pseBIS;
			mjsg[i].psBIR = psBIR;
			mjsg[i].psBIS = psBIS;

			mjsg[i].allocationStrategy = allocationStrategy;
			mjsg[i].FS_est = FS_est;
			mjsg[i].mode = mode;
			mjsg[i].mjoins = &(mini_joins.at(i).first);
			mjsg[i].thread_id = i;
			mjsg[i].core_id = thread_core_map[i];
			mjsg[i].result = &thread_results[i];

			pthread_create( &thread_id[i], NULL, run_mini_joins_greedy4, (void*) &mjsg[i]);
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

uint64_t domain_based4(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map)
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
	Relation *pseR, *pseS, *psR, *psS, *peR, *peS, *pcR, *pcS;
	BucketIndex *pseBIR, *pseBIS, *psBIR, *psBIS;
	pseR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pseS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	psR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	psS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	peR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	peS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pcR  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pcS  = (Relation*) malloc( numThreads*sizeof(Relation) );
	pseBIR = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	pseBIS = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	psBIR = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	psBIS = (BucketIndex*) malloc( numThreads * sizeof(BucketIndex) );
	uint64_t FS_est = create_partitions4( R, S, pseR, pseS, psR, psS, peR, peS, pcR, pcS, pseBIR, pseBIS, psBIR, psBIS, domain_start, domain_end, numGranules, granuleExtent, numThreads, allocationStrategy, thread_core_map);
	printf("--------------------------\n");

	// join
	printf("\n--------- JOIN ---------\n");
	double joinTime;
	for (uint32_t i = 0; i < computations; i++)
	{
		tim.start();
		result = ParallelDomainBased_Execute4( pseR, pseS, psR, psS, peR, peS, pcR, pcS, max_group_size, pseBIR, pseBIS, psBIR, psBIS, numThreads, mode, FS_est, allocationStrategy, thread_core_map);
		joinTime = tim.stop();
		printf("Total Joining:              %lf secs\n", joinTime);
	}
	printf("------------------------\n");

	// free memory
	for (uint32_t i = 0; i < numThreads; i++)
	{
		free(pseBIR[i].bucket_list_decomposed);
		free(pseBIS[i].bucket_list_decomposed);
		free(psBIR[i].bucket_list_decomposed);
		free(psBIS[i].bucket_list_decomposed);

		if (allocationStrategy != OS)
		{
			if ( (pseR[i].numRecords*sizeof(Timestamp)) && (pseR[i].startpoints) ) numa_free( pseR[i].startpoints, pseR[i].numRecords*sizeof(Timestamp) );
			if ( (pseR[i].numRecords*sizeof(Timestamp)) && (pseR[i].endpoints)   ) numa_free( pseR[i].endpoints,   pseR[i].numRecords*sizeof(Timestamp) );
			if ( (psR[i].numRecords*sizeof(Timestamp)) && (psR[i].startpoints) ) numa_free( psR[i].startpoints, psR[i].numRecords*sizeof(Timestamp) );
			if ( (psR[i].numRecords*sizeof(Timestamp)) && (psR[i].endpoints)   ) numa_free( psR[i].endpoints,   psR[i].numRecords*sizeof(Timestamp) );
			if ( (pseS[i].numRecords*sizeof(Timestamp)) && (pseS[i].startpoints) ) numa_free( pseS[i].startpoints, pseS[i].numRecords*sizeof(Timestamp) );
			if ( (pseS[i].numRecords*sizeof(Timestamp)) && (pseS[i].endpoints)   ) numa_free( pseS[i].endpoints,   pseS[i].numRecords*sizeof(Timestamp) );
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
			free(pseR[i].startpoints);
			free(pseS[i].startpoints);
			free(psR[i].startpoints);
			free(psS[i].startpoints);
			free(peR[i].startpoints);
			free(peS[i].startpoints);
			free(pcR[i].startpoints);
			free(pcS[i].startpoints);

			free(pseR[i].endpoints);
			free(pseS[i].endpoints);
			free(psR[i].endpoints);
			free(psS[i].endpoints);
			free(peR[i].endpoints);
			free(peS[i].endpoints);
			free(pcR[i].endpoints);
			free(pcS[i].endpoints);
		}

		free(pseR[i].record_list);
		free(pseS[i].record_list);
		free(psR[i].record_list);
		free(psS[i].record_list);
		free(peR[i].record_list);
		free(peS[i].record_list);
		free(pcR[i].record_list);
		free(pcS[i].record_list);
	}
	free(pseBIR);
	free(pseBIS);
	free(psBIR);
	free(psBIS);
	free(pseR);
	free(pseS);
	free(psR);
	free(psS);
	free(peR);
	free(peS);
	free(pcR);
	free(pcS);

	return result;
}
