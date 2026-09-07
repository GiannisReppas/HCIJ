#include "def.hpp"
#include "containers/relation.hpp"

// function to print usage message about the program
void usage();

// function used to parse input given from command line
void parse_command_line( int argc, char* argv[], int& mode, int& computations, int& max_group_size, int& numThreads, int& allocationStrategy, bool& single_threaded, bool& domain_based);

// single thread execution optFS
uint64_t optFS(Relation& R, Relation& S, int mode, int computations, int max_group_size, int allocationStrategy, int* thread_core_map);

// multi-threaded techniques
uint64_t domain_based3(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map);
uint64_t domain_based4(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map);
uint64_t parallel_FSs(Relation& R, Relation&S, int mode, int computations, int max_group_size, int numThreads, int allocationStrategy, int* thread_core_map);

// helper function to set the map between threads and their execution cores
void set_thread_core_map(int* thread_core_map);

int main(int argc, char *argv[])
{
	Relation R, S;
	int mode = -1, computations = 1, max_group_size = std::numeric_limits<int>::max(), numThreads = 1, allocationStrategy = OS;
	bool single_threaded = true, db = false;
	uint64_t result = 0;

	// Read command line
	parse_command_line( argc, argv, mode, computations, max_group_size, numThreads, allocationStrategy, single_threaded, db);
	printf("**********************\n");

	// Load inputs
	// Use 2 parallel threads to load input
	printf("Loading input...\n");
	pthread_t thread_id[2];
	struct LoadRelationStructure lrs[2];
	lrs[0].rel = &R;
	lrs[0].filename = argv[ argc-2 ];
	lrs[0].allocationStrategy = allocationStrategy;
	lrs[1].rel = &S;
	lrs[1].filename = argv[ argc-1 ];
	lrs[1].allocationStrategy = allocationStrategy;
	pthread_create( &thread_id[0], NULL, &Relation::load_helper, (void*) &lrs[0]);
	pthread_create( &thread_id[1], NULL, &Relation::load_helper, (void*) &lrs[1]);
	pthread_join( thread_id[0], NULL);
	pthread_join( thread_id[1], NULL);
	printf("Input loaded.\n\n");

	// print setup of running experiment
	printf("---------- SETUP ----------\n");
	printf("FilenameR:                  %s\n", lrs[0].filename);
	printf("FilenameS:                  %s\n", lrs[1].filename);
	printf("Mode: ");
	if (mode == WORKLOAD_COUNT)
		printf("                      WORKLOAD_COUNT\n");
	else if (mode == XOR)
		printf("                      XOR\n");
	else if (mode == SIMD_XOR)
		printf("                      SIMD_XOR\n");
	printf("Max. group size:            %d\n", max_group_size);
	printf("Numa strategy: ");
	if (allocationStrategy == OS)
		printf("             OS\n");
	else if (allocationStrategy == CLOSEST)
		printf("             CLOSEST\n");
	else if (allocationStrategy == OPPOSITE)
		printf("             OPPOSITE\n");
	printf("Parallel processing: ");
	if (single_threaded)
	{
		printf("       disabled\n");
	}
	else
	{
		printf("       enabled, ");
		if (db)
			printf("(Domained-based Partitioning)\n");
		else
			printf("(FS-based Scheduling)\n");
	}
	printf("Threads:                    %d\n", numThreads);
	int thread_core_map[24];
	set_thread_core_map( thread_core_map );
	printf("---------------------------\n\n");

	// execute
	Timer tim;
	tim.start();
	if (single_threaded == true)
	{
		result = optFS( R, S, mode, computations, max_group_size, allocationStrategy, thread_core_map);
	}
	else
	{
		if (db)
		{
			result = domain_based3( R, S, mode, computations, max_group_size, numThreads, allocationStrategy, thread_core_map);
		}
		else
		{
			result = parallel_FSs( R, S, mode, computations, max_group_size, numThreads, allocationStrategy, thread_core_map);
		}
	}
	tim.stop();

	// results
	printf("\n\nResult:                              %" PRIu64 "\n", result);
	printf("Total time (Excluding loading time): %lf seconds\n", tim.stop());
	printf("**********************\n\n");

	return 0;
}

void usage()
{
	printf("Usage:\n");
	printf("ij -m mode -c computations -g max_group_size -t numThreads -n numaStrategy -d [FILE1] [FILE2]\n");
	printf("-- m determines the way to compute the final result (see later for options).\n");
	printf("-- c determines the number of times that the join will be computed. It is not mandatory and is set on 1 by default. --\n");
	printf("-- g determines the maximum group size in every Forward Scan of bgudFS. It it not mandatory and is set on max_int by default. --\n");
	printf("-- t determines the number of threads that will be used. It enables/disables parallel processing if used/not used. --\n");
	printf("-- n determines the NUMA strategy to be used. It is not mandatory and is set on OS by default. --\n");
	printf("-- d should only be used in case the user chooses the Domain-based Partitioning for parallel processing.--\n");

	printf("\nAvailable modes: wc, xor, simd_xor\n");
	printf("wc computes only the number of matches, not real matches\n");
	printf("xor computes the sum of the xor between the startpoints of every match\n");
	printf("simd_xor computes the sum of the xor between the startpoints of every match, using SIMD\n");

	printf("\nAvailable NUMA strategies: os, closest, opposite\n");
	printf("! BE CAREFULL TO DEFINE A CORRECT THREAD-TO-CORE MAP IN custom_numa_functions.cpp (DEPENDING ON YOUR SYSTEM) !\n");
}

void parse_command_line( int argc, char* argv[], int& mode, int& computations, int& max_group_size, int& numThreads, int& allocationStrategy, bool& single_threaded, bool& domain_based)
{
	// Read command line
	char c;
	while ((c = getopt(argc, argv, "m:c:g:t:n:d")) != -1)
	{
		switch(c)
		{
			case 'm':
				if ( strcmp(optarg,"wc") == 0 )
				{
					mode = WORKLOAD_COUNT;
				}
				else if ( strcmp(optarg,"xor") == 0 )
				{
					mode = XOR;
				}
				else if ( strcmp(optarg,"simd_xor") == 0 )
				{
					mode = SIMD_XOR;
				}
				else
				{
					usage();
					exit(1);
				}
				break;
			case 'c':
				computations = atoi(optarg);
				if (computations <= 0)
				{
					usage();
					exit(1);
				}
				break;
			case 'g':
				max_group_size = atoi(optarg);
				if (max_group_size <= 0)
				{
					usage();
					exit(1);
				}
				break;
			case 't':
				single_threaded = false;
				numThreads = atoi(optarg);
				if (numThreads <= 0)
				{
					usage();
					exit(1);
				}
				break;
			case 'd':
				domain_based = true;
				break;
			case 'n':
				if ( strcmp(optarg,"os") == 0 )
				{
					allocationStrategy = OS;
				}
				else if ( strcmp(optarg,"closest") == 0 )
				{
					allocationStrategy = CLOSEST;
				}
				else if ( strcmp(optarg,"opposite") == 0 )
				{
					allocationStrategy = OPPOSITE;
				}
				else
				{
					usage();
					exit(1);
				}
				break;
			default:
				usage();
				exit(1);
		}
	}
	if (mode == -1)
	{
		usage();
		exit(1);
	}
	if ( (single_threaded == true) && (domain_based == true) )
	{
		printf("-d parameter only for parallel processing\n");
		exit(1);
	}
}
