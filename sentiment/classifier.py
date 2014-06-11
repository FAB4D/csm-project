from sklearn.svm import LinearSVC
import sklearn.svm as sksvm
from sklearn.grid_search import GridSearchCV
from sklearn.multiclass import OneVsRestClassifier

from mongo_retriever import mongoDB
from sklearn.externals import joblib

from datetime import date, datetime

import svm
from svmutil import *

import sys
import csv 

from itertools import *

import numpy as np

class Classifier():

    def __init__(self):
        self.db = mongoDB()

    def create_features(self, labelled_tweets, mode):
        if mode == 'write':
            featuref = csv.writer(open('data/features.csv', 'wb'), delimiter=',', quotechar='"')
            self.features = []
            print 'creating feature list...'
            print
            for tweet in labelled_tweets:
                tid = tweet[0]
                record = self.db.fetch('feature-collection', { "tid_str": tid }, False)[0]
                if record['sentiment_words']:
                    self.features.extend(record['sentiment_words'])

            print 'finished creating feature list!'
            print 
            self.features = sorted(list(set(self.features)))
            print self.features
            for feature in self.features:
                featuref.writerow([feature])
        else:                
            featuref = csv.reader(open('data/features.csv', 'rb'), delimiter=',', quotechar='"')
            self.features = []
            for feature in featuref:
                self.features.append(feature)
            self.features = sorted(self.features)

    def extract_feature(self, tid, sentiment):
        label = 0
        record = self.db.fetch('feature-collection', {'tid_str': tid}, False)[0]
        words = record['sentiment_words']
        if words:
            feature_vec = []
            for word in self.features:
                if word[0] in words:
                    feature_vec.append(1)
                    continue
                feature_vec.append(0)
        else:
            feature_vec = np.zeros(len(self.features)).tolist()

        if sentiment == 'positive':
            label = 0
        elif sentiment == 'negative':
            label = 1
        elif sentiment == 'neutral':
            label = 2

        return (feature_vec, label)

    def preprocess_data(self, source, load):
        if not load:
            labelled_tweet_set= csv.reader(open('data/%s'%source, 'rb'), delimiter=',', quotechar='"')
            #old = csv.reader(open('data/tweets_to_classify.csv', 'rb'), delimiter=',', quotechar='"')
            tweet_ids = []
            #for o in old:
            #    tweet_ids.append(o[2])
            #csv_f = csv.writer(open('data/labelled_tweets_mod.csv', 'wb'), delimiter=',')
            labelled_tweets = []
            #count = 0
            for tweet in labelled_tweet_set:
                #print tweet[0].strip('\'').strip('\"')
                #csv_f.writerow([tweet[0].strip('\'').strip('\"'), tweet[1], tweet_ids[count], tweet[3]])
                #count+=1
                if tweet[1]:
                    labelled_tweets.append((tweet[2], tweet[1]))

            print 'creating tweet features..'
            self.create_features(labelled_tweets, 'read')
            self.data = []
            self.labels = []
            for tweet in labelled_tweets:
                feature, label = self.extract_feature(tweet[0], tweet[1])
                self.data.append(feature)
                self.labels.append(label)

            self.data = np.array(self.data)
            print 'data:'
            print self.data
            self.labels = np.array(self.labels)
            print 'labels'
            print self.labels

            np.save('data', self.data)
            np.save('labels', self.labels)
        else:
            self.data = np.load('data.npy')
            self.labels = np.load('labels.npy')
            print self.data.shape
            print self.labels.shape

            neutrali = np.argwhere(self.labels==2)
            negativi = np.argwhere(self.labels==1)
            print len(neutrali)
            print len(negativi)
            print len(np.argwhere(self.labels==0))

            print self.data[neutrali[0]].shape
            print self.data[negativi[0]].shape

            print np.argwhere(self.data[negativi[0]] == 1)

    def split_data(self):
        self.xtrain = self.data[0:1000]
        self.ytrain = self.labels[0:1000]
        print self.xtrain.shape

        self.xtest = self.data[1000:]
        self.ytest = self.labels[1000:]

    def select_model(self, params, folds):
        self.type = sksvm.SVC()
        self.clf = GridSearchCV(self.type, params, cv=folds)
        self.clf.fit(self.xtrain,self.ytrain)

        print "Best parameters set found on development set:"
        print self.clf.best_estimator_
        print self.clf.best_score_
        print
        print "Grid scores on development set:"
        for params, mean_score, scores in self.clf.grid_scores_:
            print("%0.3f (+/-%0.03f) for %r"% (mean_score, scores.std() / 2, params))
            print

        self.clf = self.clf.best_estimator_

    def train_libsvm(self):
        problem = svm_problem(self.ytrain.tolist(), self.xtrain.tolist())
        param = svm_parameter()
        param.kernel_type = LINEAR
        self.clf = svm_train(problem, param)
        svm_save_model('dump.txt', self.clf)

    def test_libsvm(self):
        p_labels, p_accs, p_vals = svm_predict([0] * len(self.ytest.tolist()),self.xtest.tolist(), self.clf)
        print p_labels, p_accs, p_vals

    def train(self, params, folds):
        try:
            self.clf = joblib.load("classifier.pkl")
            print "using previously trained model"
        except:
            print "building new model"
            self.select_model(params, folds) 
            joblib.dump(self.clf,"classifier.pkl")

        self.clf.fit(self.xtrain, self.ytrain)

    def test(self):
        print self.clf.predict(self.xtest)
        print self.ytest

    def classify(self):
        weather_records = self.db.fetch_sorted('weather-collection', {}, 'created_at', 'ASC')

        date_dict = {}
        for record in weather_records:
            curr_date = date(int(record['date']['year']), int(record['date']['month']),int(record['date']['day']))
            print curr_date
            begin_date = datetime(curr_date.year,curr_date.month,curr_date.day, 00, 00, 00)
            end_date = datetime(curr_date.year,curr_date.month,curr_date.day, 23, 59, 59)
            records = self.db.fetch('feature-collection', { 'created_at' : { "$gte": begin_date, "$lte": end_date}}, False)

            if curr_date not in date_dict:
                date_dict[curr_date] = {}
                date_dict[curr_date]['weather'] = record['data']
                date_dict[curr_date]['features'] = []
                for record in records:
                    date_dict[curr_date]['features'].append(record['sentiment_words'])
            else:
                print 'weather date already in date_dict? Dates should be unique!'

        series = []
        print 'Starting classification of tweet sentiments by date..'
        print 
        for k,v in date_dict.iteritems():
            date_features = []
            for sentiment_words in v['features']:
                feature, non_label = self.extract_feature(sentiment_words)
                date_features.append(feature)
            date_features = np.array(date_features)
            labels = self.clf.predict(date_features)

            sentiment = 0
            for l in labels:
                if label == '0':
                    sentiment += 1
                elif label == '1':
                    sentiment -= 1

            series.append((k, sentiment, v['weather']['meantemp'],v['weather']['meantemp'], v['weather']['mintemp'], v['weather']['maxtemp'], v['weather']['precipi'], v['weather']['humidity']))
            print 'finished computing sentiment for day ', k
            print 

        print series
        serieswriter = csv.writer(open('data/series.csv', 'wb'), delimiter=',', quotechar='"')
        for row in series:
            serieswriter.writerow(row)
        print 'Sentiment + Weather series successfully written to file series.csv!'

def main(args):
    # labelled_tweets.csv
    fn = args[0]
    c = Classifier()
    c.preprocess_data(fn, 'load')
    c.split_data()
    params = [{'kernel': ['rbf'], 'gamma': [1e-3, 1e-4],
                     'C': [1, 10, 100, 1000]},
                    {'kernel': ['linear'], 'C': [1, 10, 100, 1000]}]
    #c.select_model(params, 10)
    c.train(params, 10)
    c.test()
    c.classify()

    #c.train_libsvm()
    #c.test_libsvm()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1:])