#include "relation.hpp"

// numa functions
int get_closest_node(int core_id);
int get_most_distant_node(int core_id);

/**************************************************************************************************/

Record::Record()
{
}

Record::Record(Timestamp start, Timestamp end)
{
	this->start = start;
	this->end = end;
}

Record::~Record()
{
}

/**************************************************************************************************/

Relation::Relation()
{
	this->numRecords = 0;
	this->record_list = NULL;
	this->startpoints = NULL;
	this->endpoints = NULL;

	this->minStart = std::numeric_limits<Timestamp>::max();
	this->maxStart = std::numeric_limits<Timestamp>::min();
	this->minEnd   = std::numeric_limits<Timestamp>::max();
	this->maxEnd   = std::numeric_limits<Timestamp>::min();
}

void Relation::load(const char *filename)
{
	Timestamp start, end;
	std::ifstream inp(filename);
	if (!inp)
	{
		std::cerr << "error - cannot open file " << filename << std::endl;
		exit(1);
	}
	while (inp >> start >> end)
	{
		this->numRecords++;
	}
	this->record_list = (Record*) malloc(sizeof(Record)*this->numRecords);
	inp.close();

	inp.open(filename);
	size_t i = 0;
	while (i < this->numRecords)
	{
		inp >> start >> end;
		this->record_list[i] = Record(start, end);

		this->minStart = std::min(this->minStart, start);
		this->maxStart = std::max(this->maxStart, start);
		this->minEnd   = std::min(this->minEnd  , end);
		this->maxEnd   = std::max(this->maxEnd  , end);

		i++;
	}
	inp.close();
}

void Relation::columnStoreSwitch(int core_id)
{
	if (this->allocationStrategy == OS)
	{
		this->startpoints = this->numRecords ? (Timestamp*) malloc( this->numRecords*sizeof(Timestamp) ) : NULL;
		this->endpoints   = this->numRecords ? (Timestamp*) malloc( this->numRecords*sizeof(Timestamp) ) : NULL;
	}
	else if (this->allocationStrategy == CLOSEST)
	{
		this->startpoints = this->numRecords ? (Timestamp*) numa_alloc_onnode(this->numRecords*sizeof(Timestamp), get_closest_node(core_id)) : NULL;
		this->endpoints   = this->numRecords ? (Timestamp*) numa_alloc_onnode(this->numRecords*sizeof(Timestamp), get_closest_node(core_id)) : NULL;
	}
	else if (this->allocationStrategy == OPPOSITE)
	{
		this->startpoints = this->numRecords ? (Timestamp*) numa_alloc_onnode(this->numRecords*sizeof(Timestamp), get_most_distant_node(core_id)) : NULL;
		this->endpoints   = this->numRecords ? (Timestamp*) numa_alloc_onnode(this->numRecords*sizeof(Timestamp), get_most_distant_node(core_id)) : NULL;
	}
	else
	{
		printf("Something wrong with Allocation Strategy given\n");
		exit(1);
	}

	omp_set_num_threads(24);

	uint32_t n = this->numRecords;
	#pragma omp parallel for default(none) shared(n)
	for (uint32_t i = 0; i < n; i++)
	{
		this->startpoints[i] = this->record_list[i].start;
		this->endpoints[i]   = this->record_list[i].end;
	}

	free( this->record_list );
	this->record_list = NULL;
}

Relation::~Relation()
{
	free(this->record_list);
	if (this->allocationStrategy == OS)
	{
		free(this->startpoints);
		free(this->endpoints);
	}
	else
	{
		if (this->numRecords*sizeof(Timestamp) && (this->startpoints)) numa_free(this->startpoints, this->numRecords*sizeof(Timestamp));
		if (this->numRecords*sizeof(Timestamp) && (this->endpoints))   numa_free(this->endpoints,   this->numRecords*sizeof(Timestamp));
	}
}
