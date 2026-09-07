"""
This script reads the original csv file and
1) prints statistics of it
2) creates a new csv file (cleaned) which contains only valid time intervals (start<end) in integer form and valid non-temporal attributes (not null)

execution examples:
python stats_and_clean.py ./divvy/Divvy_Trips.csv 1 2 2 5 10 ./divvy_from_station_id_gender/divvy_cleaned.csv

Note: modify line 122 in case you choose a non-integer non-temporal attribute
(e.g.) for yellow trip data, trip_distance, you write:
to_write += str(int((float(row[current_non_temporal_column_index])*1000))) + ","
"""

import sys
import csv
import datetime
import pandas

# parse input
if len((sys.argv)) < 6:
	print("Usage: python stats_and_clean.py\n\t filename\n\t start_time_column_index\n\t end_time_column_index\n\t number_of_non_temporal_columns\n\t non_temporal_column_indexes\n\t cleaned_filename\n\t")
	exit(1)

filename = sys.argv[1]
start_time_column_index = int(sys.argv[2])
end_time_column_index = int(sys.argv[3])
number_of_non_temporal_columns = int(sys.argv[4])
if (number_of_non_temporal_columns + 6) != len((sys.argv)):
	print("wrong input for non-temporal columns\n")
	exit(1)
non_temporal_column_indexes = []
for i in sys.argv[5:len((sys.argv)) - 1]:
	print("once")
	non_temporal_column_indexes.append( int(i) )
cleaned_filename = sys.argv[ len((sys.argv)) - 1 ]

# Initialize

######################################
cardinality = 0

######################################
min_start = int( datetime.datetime.now().timestamp() )
max_end = 0
shortest_interval = int( datetime.datetime.now().timestamp() )
longest_interval = 0
total = 0

######################################
null_non_temporals = [0] * number_of_non_temporal_columns
invalid_start_and_end = 0

######################################
cleaned_size = 0

f = open(cleaned_filename, "w")
with open(filename, newline='') as csvfile:
	spamreader = csv.reader(csvfile, delimiter=',')
	for first_row in spamreader:
		######################################
		list_of_non_temporal_sets = []
		for i in range(0,len(first_row)):
			if (i == start_time_column_index) or (i == end_time_column_index):
				list_of_non_temporal_sets.append(None)
			else:
				x = set()
				list_of_non_temporal_sets.append( x )
		break

	# Read
	for row in spamreader:
		######################################
		cardinality += 1

		######################################
		if row[start_time_column_index] == "" or row[end_time_column_index] == "":
			start_datetime = 1
			end_datetime = 0
		else:
			start_datetime = row[start_time_column_index]
			end_datetime = row[end_time_column_index]

			if (start_datetime[20:] == "AM") and (start_datetime[11:13] == "12"):
				start_datetime = start_datetime.replace( " " + start_datetime[11:13], " 00")
			if (start_datetime[20:] == "PM") and (start_datetime[11:13] != "12"):
				start_datetime = start_datetime.replace( " " + start_datetime[11:13], " " + str(int(start_datetime[11:13]) + 12))
			if (end_datetime[20:] == "AM") and (end_datetime[11:13] == "12"):
				end_datetime = end_datetime.replace( " " + end_datetime[11:13], " 00")
			if (end_datetime[20:] == "PM") and (end_datetime[11:13] != "12"):
				end_datetime = end_datetime.replace( " " + end_datetime[11:13], " " + str(int(end_datetime[11:13]) + 12))

			# divvy , dig, taxis_chicago, street
			#start_datetime = int( pandas.Timestamp(int(start_datetime[6:10]), int(start_datetime[0:2]), int(start_datetime[3:5]), int(start_datetime[11:13]), int(start_datetime[14:16]), int(start_datetime[17:19]), 0).timestamp() )
			#end_datetime = int( pandas.Timestamp(int(end_datetime[6:10]), int(end_datetime[0:2]), int(end_datetime[3:5]), int(end_datetime[11:13]), int(end_datetime[14:16]), int(end_datetime[17:19]), 0).timestamp() )

			# taxis new york, network
			start_datetime = int( pandas.Timestamp( int(row[start_time_column_index][0:4]), int(row[start_time_column_index][5:7]), int(row[start_time_column_index][8:10]), int(row[start_time_column_index][11:13]), int(row[start_time_column_index][14:16]), int(row[start_time_column_index][17:19]), 0).timestamp() )
			end_datetime = int( pandas.Timestamp( int(row[end_time_column_index][0:4]), int(row[end_time_column_index][5:7]), int(row[end_time_column_index][8:10]), int(row[end_time_column_index][11:13]), int(row[end_time_column_index][14:16]), int(row[end_time_column_index][17:19]), 0).timestamp() )

			# emergency # start_datetime = int( pandas.Timestamp( int(row[start_time_column_index][6:]), int(row[start_time_column_index][0:2]), int(row[start_time_column_index][3:5]), 0, 0, 0, 0).timestamp() )
			# emergency # end_datetime = int( pandas.Timestamp( int(row[end_time_column_index][6:]), int(row[end_time_column_index][0:2]), int(row[end_time_column_index][3:5]), 0, 0, 0, 0).timestamp() )

		if (start_datetime >= end_datetime) or (start_datetime < 0):
			invalid_start_and_end += 1
		else:
			if start_datetime < min_start:
				min_start = start_datetime
			if end_datetime > max_end:
				max_end = end_datetime
			if (end_datetime - start_datetime) < shortest_interval:
				shortest_interval = end_datetime - start_datetime
			if (end_datetime - start_datetime) > longest_interval:
				longest_interval = end_datetime - start_datetime
			total += end_datetime - start_datetime

			to_write_flag = True;
			to_write = str(start_datetime) + "," + str(end_datetime) + ","

			if start_datetime < 0:
				print(row[start_time_column_index])
				print(start_datetime)
				exit(1)

			j = 0
			for current_non_temporal_column_index in non_temporal_column_indexes:
				if (row[current_non_temporal_column_index] == "NULL" or row[current_non_temporal_column_index] == "") and (current_non_temporal_column_index != 10):
					null_non_temporals[j] += 1
					to_write_flag = False
				else:
					if current_non_temporal_column_index == 9:
						if str(row[current_non_temporal_column_index]) == "Subscriber":
							to_write += str(0) + ","
						elif str(row[current_non_temporal_column_index]) == "Customer":
							to_write += str(1) + ","
						else:
							to_write += str(2) + ","
					elif current_non_temporal_column_index == 10:
						if str(row[current_non_temporal_column_index]) == "Male":
							to_write += str(0) + ","
						elif str(row[current_non_temporal_column_index]) == "Female":
							to_write += str(1) + ","
						else:
							to_write += str(2) + ","
					else:
						to_write += str(row[current_non_temporal_column_index]) + ","
				j += 1
			if to_write_flag:
				cleaned_size += 1
				f.write( to_write[0:len(to_write)-1] + "\n")

		######################################
		for i in range(0,len(row)):
			if (i != start_time_column_index) and (i != end_time_column_index):
				if row[i] != "NULL" and row[i] != "":
					list_of_non_temporal_sets[i].add( row[i] )
	f.close()

	# Print
	print("######################################")
	print("cardinality: " + str(cardinality) )

	print("\n######################################")
	print("minimum start point: " + str(min_start))
	print("maximum end point: " + str(max_end))
	print("domain size: " + str(max_end - min_start) )
	print("shortest interval: " + str(shortest_interval) )
	print("longest interval: " + str(longest_interval) )
	print("average interval: " + str(total/(cardinality-invalid_start_and_end)) )

	print("\n######################################")
	print(str(null_non_temporals) + " null non temporals excluded from computations")
	print(str(invalid_start_and_end) + " invalid start and end timepoints excluded from computations")

	print("\n######################################")
	print("wrote totally " + str(cleaned_size) + " tuples in the cleaned file")

	print("\n######################################")
	for i in range(0,len(first_row)):
		#if i in non_temporal_column_indexes:
		#	print(list_of_non_temporal_sets[i])
		if (i != start_time_column_index) and (i != end_time_column_index):
			print(first_row[i] + " " + str(len(list_of_non_temporal_sets[i])) )

"""
12:00 AM = 00:00
01:00 AM = 01:00
02:00 AM = 02:00
03:00 AM = 03:00
04:00 AM = 04:00
05:00 AM = 05:00
06:00 AM = 06:00
07:00 AM = 07:00
08:00 AM = 08:00
09:00 AM = 09:00
10:00 AM = 10:00
11:00 AM = 11:00
12:00 PM = 12:00
01:00 PM = 13:00
02:00 PM = 14:00
03:00 PM = 15:00
04:00 PM = 16:00
05:00 PM = 17:00
06:00 PM = 18:00
07:00 PM = 19:00
08:00 PM = 20:00
09:00 PM = 21:00
10:00 PM = 22:00
11:00 PM = 23:00
"""
