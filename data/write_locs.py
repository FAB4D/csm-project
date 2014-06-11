import json
import ast

f = open('csm-tweets-nairobi.json', 'rb')

fu = open('users.txt', 'rb')
users = fu.readlines()
fu.close()
nusers = []
for user in users:
	nuser = user.rstrip("\n")
	nusers.append(int(nuser))

tweets = f.readlines()
f.close()
print tweets[0]

locs = []
for tweet in tweets:
	raw_tweet = ast.literal_eval(tweet.rstrip("\n"))
	if raw_tweet['user']['id'] in nusers:
		raw_loc = raw_tweet['user']['location']
		loc = "".join(raw_loc.lower().split())
		locs.append(loc)
		nusers.remove(raw_tweet['user']['id'])

print len(locs)
nlocs = set(locs)
print len(nlocs)

print nlocs 
#	fw.write(str(user)+"\n")

