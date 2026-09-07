import random
import sys

startpoints_range = [0,1000000000] # 1B
size = 10000000 # 10M
lambda_1 = 1000000   # 1M
lambda_2 = 100000000 # 100M
probability_x = 0.50

f1 = open("R0_50.csv", "w")
for i in range(size):
	if random.random() < probability_x:
		r1 = startpoints_range[0]
	else:
		r1 = random.randint( startpoints_range[0] , startpoints_range[1] )
	r2 = r1 + random.randint( lambda_1 , lambda_2 )
	f1.write(str(r1) + " " + str(r2) + "\n")
f1.close()
print("R created successfully")

f1 = open("S0_50.csv", "w")
for i in range(size):
	if random.random() < probability_x:
		r1 = startpoints_range[0]
	else:
		r1 = random.randint( startpoints_range[0] , startpoints_range[1] )
	r2 = r1 + random.randint( lambda_1 , lambda_2 )
	f1.write(str(r1) + " " + str(r2) + "\n")
f1.close()
print("S created successfully")
