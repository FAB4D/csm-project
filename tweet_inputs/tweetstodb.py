# coding: UTF-8

from pymongo import MongoClient
from pymongo.errors import *
import ast
import re
import pickle
import glob
import sys
from itertools import *

import unicodecsv

from np_extractor import NPExtractor

class mongoDB:

    def __init__(self):
        self.client = MongoClient('localhost')
        self.db = self.client.tweetdb

    def insert(self, collname, obj):
        try:
            self.db[collname].insert(obj, continue_on_error = True)
            print 'Batch insert completed!\n'
        except DuplicateKeyError:
            print 'DuplicateKeyError due to mutliple insertions of users! Continue_on_error flag set to True, all inserts processed\n!'
        except DocumentTooLarge:
            print obj

"""
class c_csvwriter:
    
    def __init__(self, filename):
        self.f = open(filename, 'wb')

    def write_col(val):
        if isinstance(val, list):
            for v in val:
                v = str(v)
                self.f.write(v+',')
        else:
            val = str(val)
            self.f.write(val+',')

    def write_break():
        self.f.write('\n')

    def close():
        self.f.close()
"""

class TweetProcessor:

    def __init__(self):
        self.mongo = mongoDB()
        print 'connected to mongodb!'

        self.stopWords= self.getStopWordList('stopwords.txt')
        print 'loaded stop words!'

        self.mentions_dict = {}

        self.tweet_dictl = []
        self.user_dictl = []
        self.feature_dictl = []

	self.uids = []

        self.NPE = NPExtractor()

    def replaceTwoOrMore(self, s):
        #look for 2 or more repetitions of character and replace with the character itself
        pattern = re.compile(r"(.)\1{1,}", re.DOTALL)
        return pattern.sub(r"\1\1", s)

    def extractMentions(self, tweet):
        res = [word for word in tweet.split() if word.startswith('@')]
        if res is None:
            return None
        else: 
            names = []
            for r in res: 
                r = re.sub(r'@([^\s]+)', r'\1', r)
                r = re.sub(r'^([a-zA-Z0-9_-]*)([^a-zA-Z0-9_-]+)(.*)', r'\1', r)
                #r = re.sub(r'([a-zA-Z0-9_-]+)(\.|\")(.*)', r'\1', r)
                r = r.strip('\'\"?,.:;)!-')
                r = r.lower()
                if r == '@' or r == '':
                    continue
                names.append(r)
            return names

    def processTweet(self, tweet):
        #Convert to lower case
        tweet = tweet.lower()
        #Convert www.* or https?://* to URL
        tweet = re.sub('((www\.[\s]+)|(https?://[^\s]+)|(http?://[^\s]+))','URL',tweet)
        #Convert @username to AT_USER
        tweet = re.sub('@[^\s]+','AT_USER',tweet)
        #Remove additional white spaces
        tweet = re.sub('[\s]+', ' ', tweet)
        #Replace #word with word
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
        #trim
        tweet = tweet.strip('\'"')

        return tweet    

    def getStopWordList(self, stopWordListFileName):
        #read the stopwords file and build a list
        stopWords = []
        stopWords.append('AT_USER')
        stopWords.append('URL')
     
        fp = open(stopWordListFileName, 'r')
        line = fp.readline()
        while line:
            word = line.strip()
            stopWords.append(word)
            line = fp.readline()
        fp.close()
        return stopWords
    #end

    def getSentiWords(self, tweet):
        sentiWords = []
        #split tweet into words
        words = tweet.split()
        for w in words:
            #replace two or more with two occurrences
            w = self.replaceTwoOrMore(w)
            #strip punctuation
            w = w.strip('\'"?,.')
            #check if the word stats with an alphabet
            val = re.search(r"^[a-zA-Z][a-zA-Z0-9]*$", w)
            #ignore if it is a stop word
            if(w in self.stopWords or val is None):
                continue
            else:
                sentiWords.append(w.lower())
        return sentiWords 

    def flattenDictVal(self, dictionary):
        coords = None
        if 'coordinates' in dictionary:
            coords = dictionary.pop('coordinates')
        values = dictionary.values()
        if coords:
            values.extend(coords)

        return values

    def dictToCSV(self, pdict, t):
        values = self.flattenDictVal(pdict)
        if t == 'u':
            self.csv_u.writerow(values)
        elif t == 't':
            self.csv_t.writerow(values)

    def createDicts(self, tweet):
        user = tweet['user']

        if user['protected']:
            print "user is protected! Continuing ..."
            return

        uid = user['id_str']
        iuid = user['id']

        reply_to = ""
        if tweet['in_reply_to_user_id_str']:
            reply_to = tweet['in_reply_to_user_id_str']

        raw_loc = user['location']
        loc = "".join(raw_loc.lower().split())
        nairobi = False
        if 'nairobi' in loc:    
            nairobi = True

        self.updateMentionDict(tweet, user)
        text = self.processTweet(tweet['text']) 
        fdict = self.createFeatureDict(tweet, text, uid)
        self.feature_dictl.append(fdict)

        tdict = self.createTweetDict(tweet, text, uid, reply_to, nairobi)
        self.tweet_dictl.append(tdict)
        self.dictToCSV(tdict, 't')

        if iuid in self.uids:
            return
        else:
            self.uids.append(iuid)
            udict = self.createUserDict(user, uid, nairobi)
            self.user_dictl.append(udict)
            self.dictToCSV(udict, 'u')

    def updateMentionDict(self, tweet, user):
        mentions = self.extractMentions(tweet['text'])
        mention_list = []
        if mentions:
            for mention in mentions:
                if mention not in self.mentions_dict:
                    self.mentions_dict[mention] = {
                        "by" : [],
                        "count": 0
                    }
                mention_list = self.mentions_dict[mention]['by']
                mention_list.extend((user['screen_name'],tweet['created_at']))
                self.mentions_dict[mention]['by'] = mention_list
                self.mentions_dict[mention]['count'] = len(mention_list)

    def createFeatureDict(self, tweet, text, uid):
        sentiWords = self.getSentiWords(text)
        #print 'sentiment words: ',', '.join(sentiWords)
        #print

        topicWords = self.NPE.extract(' '.join(sentiWords))
        #print 'topic words: ',', '.join(topicWords)
        #print

        tfeature_ins = { "tid_str": tweet['id_str'],
            "sentiment_words": sentiWords, 
            "topic_words": topicWords,
            "uid_str": uid,
            "created_at": tweet['created_at']
        }
        return tfeature_ins

    def createTweetDict(self, tweet, text, uid, reply_to, nairobi):
        favorite_count = 0
        if 'favorite_count' in tweet:
            favorite_count = tweet['favorite_count']
        tweet_ins = { "tid_str": tweet['id_str'],
            "text": text, #tweet['text'].rstrip(' '),
            "favorited": favorite_count,
            "retweeted": tweet['retweet_count'],
            "created_at": tweet['created_at'],
            "coordinates": tweet['coordinates']['coordinates'],
            "uid_str": uid,
            "nairobi_loc": nairobi,
            "reply_to": reply_to
        }
        return tweet_ins 

    def createUserDict(self, user, uid, nairobi):
        # Write users to db - put field 'loc_nairobi' True/False
        user_ins = { "uid_str": uid,
            "loc": user['location'],
            "nairobi_loc": nairobi,
            "followers": user['followers_count'],
            "following": user['friends_count'],
            "screen_name": user['screen_name'],
            "desc": user['description'],
            "num_tweets": user['statuses_count'],
            "favorited": user['favourites_count'],
            "created_at": user['created_at']
        }
        return user_ins

    def batchInsertDicts(self):
        fdict = []
        fdict = self.feature_dictl
        self.mongo.insert('tweet_features', fdict)
        self.feature_dictl = []

        tdict = []
        tdict = self.tweet_dictl
        self.mongo.insert('tweet-collection', tdict)
        self.tweet_dictl = []

        udict = []
        udict = self.user_dictl
        self.mongo.insert('user-collection', udict)
        self.user_dictl = []


    def insertMentions(self):
        for k,v in self.mentions_dict.iteritems():
            ins_dict = { "screen_name": k,
                "mentioned_by": v['by'] ,
                "count": v['count']
            }
            print ins_dict
            print 
            self.mongo.insert('mention-collection', ins_dict)

    def process_tweets(self, dtype):
        self.fuser = open('users.csv', 'wb')
        self.ft = None
        self.csv_u = unicodecsv.writer(self.fuser, delimiter=',')

        if dtype == 'json':
            f = open('csm-tweets-nairobi.json', 'rb')
            raw_tweets = f.readlines()
            fjson_tweets = []
            count = 1
            print 'loading tweets from fake json\n'
            for raw_tweet in raw_tweets:
                tweet = ast.literal_eval(raw_tweet.rstrip("\n"))

                if count % 50000 == 1:
                    if self.ft is not None:
                        self.ft.close()
                    self.ft= open('tweets_%d.csv'%(count/10000+1), 'wb')
                    self.csv_t = unicodecsv.writer(self.ft, delimiter=',')

                self.createDicts(tweet)
                if count % 50000 == 0:
                    print '%d tweets processed!\n'%(count)
                    self.batchInsertDicts()

                count += 1

            self.insertMentions()
            f.close()

        elif dtype == 'pickle':
            count = 1
            for fn in glob.glob("*.pickle"):
                f = open(fn, 'rb')
                tweets = pickle.load(f)
                print 'tweets extracted from pickle!\n'
                for tweet in tweets:
                    if count % 50000 == 1:
                        if self.ft is not None:
                            self.ft.close()
                        self.ft= open('tweets_%d.csv'%(count/10000+1), 'wb')
                        self.csv_t = csv.writer(self.ft, delimiter=',')
                    self.createDicts(tweet)
                    count += 1

                self.batchInsertDicts()
                print 'another pickle processed!\n'
                f.close()

            self.insertMentions()

        else:
            print 'wrong data type!'
            return None


def main(argv):
    dtype = argv[0]
    print dtype

    tP = TweetProcessor()
    tP.process_tweets(dtype) 

if __name__ == "__main__":
    main(sys.argv[1:])
