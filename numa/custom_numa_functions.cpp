#include "../def.hpp"

int get_closest_node(int core_id)
{
	return numa_node_of_cpu(core_id);
}

int get_most_distant_node(int core_id)
{
	int closest_node = get_closest_node(core_id);

	int current_distance;
	int most_distant_node = -1;
	int max_distance = -1;

	int nodes_num = numa_max_node() + 1;
	for (uint32_t i = 0; i < nodes_num; i++)
	{
		// valgrind false memory leak of 4 bytes with numa_distance
		current_distance = numa_distance(closest_node,i);
		if ( current_distance > max_distance )
		{
			most_distant_node = i;
			max_distance = current_distance;
		}
	}

	return most_distant_node;
}

int stick_this_thread_to_core(int core_id)
{
	int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	if (core_id < 0)
		return -1;

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET( core_id % num_cores, &cpuset);

	pthread_t current_thread = pthread_self();
	return pthread_setaffinity_np( current_thread, sizeof(cpu_set_t), &cpuset);
}

void set_thread_core_map(int* thread_core_map)
{
	thread_core_map[0] = 0;
	thread_core_map[1] = 1;
	thread_core_map[2] = 2;
	thread_core_map[3] = 3;
	thread_core_map[4] = 4;
	thread_core_map[5] = 5;
	thread_core_map[6] = 6;
	thread_core_map[7] = 7;
	thread_core_map[8] = 8;
	thread_core_map[9] = 9;
	thread_core_map[10] = 10;
	thread_core_map[11] = 11;
	thread_core_map[12] = 12;
	thread_core_map[13] = 13;
	thread_core_map[14] = 14;
	thread_core_map[15] = 15;
	thread_core_map[16] = 16;
	thread_core_map[17] = 17;
	thread_core_map[18] = 18;
	thread_core_map[19] = 19;
	thread_core_map[20] = 20;
	thread_core_map[21] = 21;
	thread_core_map[22] = 22;
	thread_core_map[23] = 23;
/*
	thread_core_map[24] = 24;
	thread_core_map[25] = 25;
	thread_core_map[26] = 26;
	thread_core_map[27] = 27;
	thread_core_map[28] = 28;
	thread_core_map[29] = 29;
	thread_core_map[30] = 30;
	thread_core_map[31] = 31;
	thread_core_map[32] = 32;
	thread_core_map[33] = 33;
	thread_core_map[34] = 34;
	thread_core_map[35] = 35;
	thread_core_map[36] = 36;
	thread_core_map[37] = 37;
	thread_core_map[38] = 38;
	thread_core_map[39] = 39;
	thread_core_map[40] = 40;
	thread_core_map[41] = 41;
	thread_core_map[42] = 42;
	thread_core_map[43] = 43;
	thread_core_map[44] = 44;
	thread_core_map[45] = 45;
	thread_core_map[46] = 46;
	thread_core_map[47] = 47;
*/
}
