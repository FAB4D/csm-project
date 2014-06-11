import pymongo
from pymongo import MongoClient
from pymongo.errors import *
import unicodecsv
import re
import sys

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

    def fetch(self, collname, query, islice):
        if isinstance(islice, tuple):
            res = self.db[collname].find(query)[islice[0]: islice[1]]
        else:
            res = self.db[collname].find(query)
        return res

    def fetch_sorted(self, collname, query, sort_field, order):
        if order == 'ASC':
            return self.db[collname].find(query).sort(sort_field, pymongo.ASCENDING)
        elif order == 'DESC':
            return self.db[collname].find(query).sort(sort_field, pymongo.DESCENDING)

    def fetch_sorted_limited(self, collname, sort_field, order, l):
        print pymongo.ASCENDING
        if order == 'ASC':
            res = self.db[collname].find().sort(sort_field, pymongo.ASCENDING).limit(l)
        elif order == 'DESC':
            res = self.db[collname].find().sort(sort_field, pymongo.DESCENDING).limit(l)
        return res   
        
def main(argv):

    mdb = mongoDB()
    tweets = mdb.fetch('tweet-collection', { "nairobi_loc": True}, (300001,301200))

    f = open('tweets_to_classify.csv', 'wb')

    writer = unicodecsv.writer(f, delimiter=',')
    for tweet in tweets:
        text = re.sub('[\s]+', ' ', tweet['text'])
        writer.writerow([text,"",tweet['tid_str'],tweet['uid_str']])
        print tweet['text'], tweet['uid_str']

    f.close()

if __name__ == "__main__":
    main(sys.argv[1:])