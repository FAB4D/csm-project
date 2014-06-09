from twython import Twython, TwythonError, TwythonRateLimitError, TwythonAuthError
from datetime import datetime
from time import sleep
from sys import maxint
import re
import csv
import sys
import pickle
import traceback
import os

RATE_LIMIT_WINDOW = 15 * 60
WAIT_BETWEEN_AUTH = 10 * 60
BUFFER = 5
TWEET_RATE_LIMIT = 300
TWEET_BATCH_SIZE = 200
FOLLOWER_RATE_LIMIT = 30
FOLLOWER_BATCH_SIZE = 200
MIN_TWEETS = 30 
MAX_TWEETS = 3200
MAX_FOLLOWERS = 200000
MAX_TWEETS_PER_FILE = 5000
MAX_USERS_PER_FILE = 5000

input_folder = ""
output_folder = "tweet_tmp_store/"
users_folder = "geo_users/"

processed_followers = 0
nairobi_followers = 0
geo_followers = 0

def check_clock(last_time):
    elapsed_time = (datetime.now() - last_time).seconds
    if elapsed_time < RATE_LIMIT_WINDOW:
        write_log('Sleeping for %s seconds...\n' % (RATE_LIMIT_WINDOW - elapsed_time))
        sleep(RATE_LIMIT_WINDOW - elapsed_time)

def write_log(message):
    f_log = open("log.txt", 'a')
    f_log.write(message)
    f_log.close()

def data_dump(data, file_name):
    f = open(file_name, 'wb')
    pickle.dump(data, f)
    f.close()

def authenticate(APP_KEY, APP_SECRET):
    while True:
        write_log('Authenticating to Twitter...\n')
        try:
            twitter = Twython(APP_KEY, APP_SECRET, oauth_version=2)
            ACCESS_TOKEN = twitter.obtain_access_token()
            ret = Twython(APP_KEY, access_token=ACCESS_TOKEN)
            write_log('Authentication successful\n')
            return ret
        except TwythonAuthError, e:
            traceback.print_exc()
            sleep(WAIT_BETWEEN_AUTH)

def get_good_followers(followers, min_tweets):
    global processed_followers;
    global nairobi_followers;
    global geo_followers;
    fewtweets = 0; nolocation = 0; badlocation = 0; accepted = 0; protected = 0; geo_enabled = 0;

    for follower in followers:
        good_location = False
        if follower['statuses_count'] < min_tweets:
##            print 'Skipping "%s" because he/she/it has only %d tweets' % (
##                    follower['screen_name'], follower['statuses_count'])
            fewtweets += 1
            continue
        if not follower['location']:
##            print 'Skipping "%s" because he has no location' % (
##                    follower['screen_name'])
            nolocation += 1
            continue
        if follower['protected']:
##            print 'Skipping "%s" because he is protected' % (
##                    follower['screen_name'])
            protected += 1
            continue
        for location_part in re.split('[ ,]+', re.sub(r'[^a-zA-Z, ]', '', follower['location']).strip()):
            if 'nairobi' in location_part.lower():
                good_location = True
                break
        if good_location:
            accepted += 1
            if follower['geo_enabled']:
                geo_enabled += 1
                nairobi_followers += 1
                geo_followers += 1
                yield follower
        else:
            badlocation += 1
        processed_followers += 1


##            print 'Skipping "%s" because his location is ' \
##                    '"%s", and I do not have that in my list' % (
##                            follower['screen_name'], follower['location'])
    #print
    #print 'Stats: '
    print 'followers ',len(followers)
    print 'location accepted ',accepted
    print 'geo_enabled ',geo_enabled
    """
    for str, val in (('Bad location', badlocation),
                     ('No location', nolocation),
                     ('Too few tweets', fewtweets),
                     ('Accepted', accepted),
                     ('Protected', protected)):
        print '\t%s: %d (%.02f%%)' % (str, val, val*100./len(followers))
    """

def get_followers(user_id, APP_KEY, APP_SECRET):
    twitter = authenticate(APP_KEY, APP_SECRET)

    num_followers = min(MAX_FOLLOWERS, twitter.show_user(id=user_id)['followers_count'])
    next_cursor = -1; num_requests = 0; users_downloaded = 0; num_good_followers = 0; page_number = 1; file_count = 0

    last_time = datetime.now()
    time_start = datetime.now()

    good_followers = []

    while users_downloaded < num_followers:
        write_log('Downloading followers page %d for %d\n' % (page_number, user_id))
        try:
            response = twitter.get_followers_list(id=user_id, count=FOLLOWER_BATCH_SIZE, cursor=next_cursor)            
            followers = response['users']
        except TwythonRateLimitError:
            write_log('Sleeping...\n')
            sleep(RATE_LIMIT_WINDOW)
            continue
        except TwythonError:
            continue
        except KeyError:
            continue

        size_good_followers = len(good_followers)
        good_followers.extend((list(get_good_followers(followers, MIN_TWEETS))))
        num_good_followers += len(good_followers) - size_good_followers
        
        num_requests += 1
        if num_requests == FOLLOWER_RATE_LIMIT:
            check_clock(last_time)
            last_time = datetime.now()
            num_requests = 1
        
        next_cursor = response['next_cursor']
        page_number += 1
        users_downloaded += FOLLOWER_BATCH_SIZE

        if len(good_followers) >= MAX_USERS_PER_FILE:
            file_count += 1
            data_dump(good_followers, users_folder + '%d_%s.pickle'%(user_id, file_count))
            good_followers = []
        
        write_log(('%.02f%%\n')%(100.0 * users_downloaded / num_followers))
        write_log('%s good followers collected\n'%(num_good_followers))

    file_count += 1
    data_dump(good_followers, users_folder + '%d_%s.pickle'%(user_id, file_count))
    duration = (datetime.now() - time_start).seconds
    write_log('Number of good followers listed %s in %s seconds\n' % (num_good_followers, duration))

def run_get_followers(APP_KEY, APP_SECRET):
    global processed_followers;
    global nairobi_followers;
    global geo_followers;
    os.chdir('tweet_inputs')
    f = open('nairobi_users.txt', 'rb')
    users = f.readlines()

    processed = 0
    get=False
    for user in users:
        uid = int(user.rstrip('\n'))
        print 'processing %d ...\n'%uid
        if uid != 184713767 and get is False:
            continue
        get = True
        get_followers(uid, APP_KEY, APP_SECRET) 
        processed += 1
        if processed % 10 == 0:
            print 'users processed ',processed
            print 'processed followers ',processed_followers
            print 'nairobi followers ',nairobi_followers
            print 'geo enabled followers ',geo_followers

def get_tweets(APP_KEY, APP_SECRET):
    twitter = authenticate(APP_KEY, APP_SECRET)
    f_twids = open('tweets_ids.txt', 'rb')
    lines = f_twids.readlines()
    twids = []
    for line in lines:
       twids.append(int(line.rstrip('\n')))

    f_twids.close()
    
    users = []
    f_users= open('nairobi_users.txt', 'rb')
    lines = f_users.readlines()
    for line in lines:
	   users.append(int(line.rstrip('\n')))

    f_users.close()
    
    num_requests = 0; file_count = 1; total_tweets = 0; user_num = 0
    last_time = datetime.now(); time_start = datetime.now(); log_time = datetime.now()
    tweets = []
    
    write_log('%s followers available\n'%(len(users)))
    
    for uid in users:
        root = uid
        write_log('Trying user id %d\n'%(uid))
        try:
            user = twitter.show_user(id=uid)        
        except TwythonError:
            print 'Twitter API returned a 404 (Not Found). User probably suspended.\n'
            continue

        num_tweets = min(user['statuses_count'], MAX_TWEETS)
        write_log('Collecting %s tweets from %s id %d\n'%(num_tweets, user['screen_name'], uid))
        max_tweet_id = 0; tweets_collected = 0
        
        while tweets_collected < num_tweets:
            new_tweets = []
            if num_requests == TWEET_RATE_LIMIT:
                check_clock(last_time)
                last_time = datetime.now()
                num_requests = 1
            try:
                num_requests += 1
                if max_tweet_id == 0:
                    new_tweets = twitter.get_user_timeline(screen_name=user['screen_name'], count=TWEET_BATCH_SIZE)
                else:
                    new_tweets = twitter.get_user_timeline(screen_name=user['screen_name'], max_id=max_tweet_id, count=TWEET_BATCH_SIZE)
                tweets_collected += TWEET_BATCH_SIZE
                #total_tweets += len(new_tweets)
            except TwythonRateLimitError:
                write_log('Rate limit error - sleeping...\n')
                sleep(RATE_LIMIT_WINDOW)
                last_time = datetime.now()
                num_requests = 1
                continue
            except TwythonError:
                break
            for tweet in new_tweets:
                if max_tweet_id == 0 or max_tweet_id > int(tweet['id']):
                    max_tweet_id = int(tweet['id'])
                if tweet['coordinates'] and int(tweet['id']) not in twids:
                    print 'tweet geolocated'
                    tweets.extend(tweet)
                    total_tweets += 1

        print total_tweets
            
        if total_tweets >= file_count * MAX_TWEETS_PER_FILE:
            data_dump(tweets,output_folder+'%s_tweets_%s.pickle'%(uid, file_count + 1000))
            file_count += 1
            tweets = []

        user_num += 1
        
        write_log(('%s tweets collected\n')%(total_tweets))
        write_log(('%.02f%%\n')%(100.0 * user_num / len(users)))
        
    data_dump(tweets,output_folder+'%s_tweets_%s.pickle'%(root, file_count + 1000))
    duration = (datetime.now() - time_start).seconds
    write_log('Number of tweets collected %s in %s seconds\n' % (total_tweets, duration))
    
def main():
    os.chdir('nairobi_tweets')
    APP_KEY = sys.argv[1]
    print APP_KEY
    APP_SECRET = sys.argv[2]
    print APP_SECRET
    option = sys.argv[3]
        
    if option == 'users' or option == 'both':
        run_get_followers(APP_KEY, APP_SECRET)
    if option == 'tweets' or option == 'both':
        get_tweets(APP_KEY, APP_SECRET)
    
if __name__ == '__main__':
    main()
