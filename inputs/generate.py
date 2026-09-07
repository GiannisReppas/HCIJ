"""
This script reades the cleaned csv file (3 columns) and randomly picks tuples to put in the new relations
Necessary input is the
	1) name and size of the cleaned csv file (known after running stats_and_clean.py)
	2) the name of the new relations, and the size of the biggest new relation along with the step substructed from it to create the new relations
	   (e.g. size 500000 with 50000 as step will produce 450000,400000,... as new relations)
	   Make sure files don't exist already before running the script
	3) min start point from cleaned csv file to create intervals with small integers (known after running stats_and_clean.py)

execution examples:
python generate.py citibike_cleaned.csv 1146732 Citibike 500000 50000 1490994058
"""

import sys
import csv
import random
import shutil

if len((sys.argv)) != 7:
	print("Usage: python generate.py\n\t cleaned_csv_filename\n\t cleaned_csv_size\n\t tables_filename\n\t max_table_size\n\t step\n\t min_start_point\n\t")
	exit(1)

cleaned_csv_filename = sys.argv[1]
cleaned_csv_size = int(sys.argv[2])
tables_filename = sys.argv[3]
max_table_size = int(sys.argv[4])
step = int(sys.argv[5])
min_start_point = int(sys.argv[6])

if max_table_size*2 >= cleaned_csv_size:
	print("\nmax_table_size*2 must be smaller than cleaned_csv_size\n")
	exit(1)

full_file = []
with open(cleaned_csv_filename, newline='') as csvfile:
	spamreader = csv.reader(csvfile, delimiter=',')
	for row in spamreader:
		full_file.append(row)

all_row_ids = list(range(0,cleaned_csv_size))
random.shuffle(all_row_ids)

r_row_ids = all_row_ids[0:max_table_size]
s_row_ids = all_row_ids[max_table_size:2*max_table_size]

file_count = 1
previous_filename = ""
current_filename = tables_filename + "R" + str(int(step/1000)*file_count) + "K.csv"
f = open( current_filename, "a")
i = 0
for r_row_id in r_row_ids:
	to_write = full_file[r_row_id]

	f.write( str(int(to_write[0])-min_start_point) + " " + str(int(to_write[1])-min_start_point) + "\n" )

	i += 1
	if (i % step) == 0:
		file_count += 1
		previous_filename = current_filename
		current_filename = tables_filename + "R" + str(int(step/1000)*file_count) + "K.csv"

		f.close()
		if i != max_table_size:
			shutil.copyfile( previous_filename, current_filename)
			f = open( current_filename, "a")

file_count = 1
previous_filename = ""
current_filename = tables_filename + "S" + str(int(step/1000)*file_count) + "K.csv"
f = open( current_filename, "a")
i = 0
for s_row_id in s_row_ids:
	to_write = full_file[s_row_id]

	#f.write( str(int(to_write[0])-min_start_point) + " " + str(int(to_write[1])-min_start_point) + " " + to_write[2] + " " + to_write[3] + "\n")
	f.write( str(int(to_write[0])-min_start_point) + " " + str(int(to_write[1])-min_start_point) + "\n" )

	i += 1
	if (i % step) == 0:
		file_count += 1
		previous_filename = current_filename
		current_filename = tables_filename + "S" + str(int(step/1000)*file_count) + "K.csv"

		f.close()
		if i != max_table_size:
			shutil.copyfile( previous_filename, current_filename)
			f = open( current_filename, "a")
