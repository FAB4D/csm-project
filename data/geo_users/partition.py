import json
import ast
import math

f = open('unique_users.txt', 'rb')

nairobi = f.readlines()

users = []

for uo in nairobi:
	users.append(uo.rstrip('\n'))

partlen = math.ceil(len(users)/5.0)

count = 0
part = 1 
fw = open('unique_users'+str(part)+'.txt', 'wb')
for user in users:
	if count % partlen == 0 and count != 0:
		fw.close()
		part+= 1
		fw = open('unique_users'+str(part)+'.txt', 'wb')

	fw.write(str(user)+"\n")
	count += 1

fw.close()