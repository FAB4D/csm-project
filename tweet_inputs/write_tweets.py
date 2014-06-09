import ast
import pickle
import os

def data_dump(data, file_name):
    f = open(file_name, 'wb')
    pickle.dump(data, f)
    f.close()

def main():
	f = open('csm-tweets-nairobi.json', 'rb')
	fu = open('nairobi_users.txt', 'rb')

	tweets_raw = f.readlines()
	f.close()
	lines = fu.readlines()
	fu.close()

	users = []
	for line in lines:
		users.append(int(line.rstrip('\n')))

	loc_tweets = []
	tweets = []

	os.chdir('tweets')
	count_loc = 0
	count = 0

	print len(users)
	for tweet in tweets_raw:
		print count	
		print count_loc
		raw_tweet = ast.literal_eval(tweet.rstrip("\n"))
		print raw_tweet
		break

		if raw_tweet['user']['id'] in users:
			loc_tweets.extend(raw_tweet)
			count_loc += 1
		else:
			tweets.extend(raw_tweet)
			count += 1

		if count_loc % 5000 == 0 and count_loc != 0:
			data_dump(loc_tweets, 'nairobi_loc_%d.pickle'%(count_loc/5000))
			nairobi_loc_tweets = []

		if count % 5000 == 0 and count != 0:
			data_dump(tweets, 'nairobi_%d.pickle'%(count/5000))
			tweets = []


	data_dump(loc_tweets, 'nairobi_loc_%d.pickle'%(count_loc/5000+1))
	data_dump(tweets,'nairobi_%d.pickle'%(count/5000+1))


if __name__ == "__main__":
	main(sys.argv[1:])