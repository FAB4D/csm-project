import json
import ast

f = open('csm-tweets-nairobi.json', 'rb')

tweets = f.readlines()
print tweets[0]

users = []
fw = open('nairobi_users.txt', 'wb')
for tweet in tweets:
	raw_tweet = ast.literal_eval(tweet.rstrip("\n"))
	raw_loc = raw_tweet['user']['location']
	loc = "".join(raw_loc.lower().split())
	if 'nairobi' in loc:
		print loc
		users.append(raw_tweet['user']['id'])

print len(users)
unusers = set(users)
print len(unusers)

for user in unusers:
	fw.write(str(user)+"\n")

