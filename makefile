SOURCES =   containers/relation.cpp \
			containers/bucket_index.cpp \
			containers/FS_map.cpp \
			serial_executions/FS_estimation.cpp \
			serial_executions/udFS.cpp \
			serial_executions/bgudFS.cpp \
			serial_executions/optFS.cpp \
			numa/custom_numa_functions.cpp \
			parallel_executions/FS_based/FS_direct_discovery.cpp \
			parallel_executions/FS_based/FS_intermediate_discovery.cpp \
			parallel_executions/FS_based/parallel_FSs.cpp \
			parallel_executions/Domain_based/domain_based3.cpp \
			parallel_executions/Domain_based/domain_based4.cpp \
			parallel_executions/Domain_based/create_partitions3.cpp \
			parallel_executions/Domain_based/create_partitions4.cpp

OBJECTS = $(SOURCES:.cpp=.o)

all: main

main: $(OBJECTS)
	g++ -O3 -std=c++20 $(OBJECTS) main.cpp -o ij -lnuma -ltbb -fopenmp -mavx512f -march=native
#for vtune: -g -fno-omit-frame-pointer
#for roofline: -g -fno-omit-frame-pointer -fno-inline -fno-unroll-loops -march=skylake-avx512

.cpp.o:
	g++ -O3 -std=c++20 -c $< -o $@ -lnuma -ltbb -fopenmp -mavx512f -march=native
#for vtune: -g -fno-omit-frame-pointer
#for roofline: -g -fno-omit-frame-pointer -fno-inline -fno-unroll-loops -march=skylake-avx512

clean:
	rm -rf containers/*.o
	rm -rf serial_executions/*.o
	rm -rf numa/*.o
	rm -rf parallel_executions/FS_based/*.o
	rm -rf parallel_executions/Domain_based/*.o
	rm -rf ij
