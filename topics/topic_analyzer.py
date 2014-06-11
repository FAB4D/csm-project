from mongo_retriever import mongoDB
import operator
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from pytz import *
EAT = timezone('Africa/Nairobi')

import csv

class TopicAnalyzer:

    def __init__(self):
        self.mongo = mongoDB()

    def diff_month(self, d1, d2):
        return (d1.year - d2.year)*12 + d1.month - d2.month

    def get_first_last_date(self):
        last_date = self.mongo.fetch_sorted_limited('tweet-collection','created_at', 'DESC', 1)[0]
        last_date = last_date['created_at']

        first_date = self.mongo.fetch_sorted_limited('tweet-collection','created_at', 'ASC', 1)[0]
        first_date = first_date['created_at']

        return (first_date, last_date)

    def convertDate(self, date):
        return EAT.localize(date)

    def topicsByTotalPeriod(self, limit):
        writer = csv.writer(open('topics_by_period.csv', 'wb'), delimiter=',', quotechar='"')
        topic_dict = {}
        res = self.mongo.fetch('feature-collection-mod', {}, False)
        for record in res:
            if record['topic_words']:
                topic_words = record['topic_words'][0].split()
                for topic in topic_words:
                    if topic in topic_dict:
                        topic_dict[topic] += 1
                    else:
                        topic_dict[topic] = 1

        sorted_topics = sorted(topic_dict.iteritems(), key=operator.itemgetter(1), reverse = True)
        count = 1
        writer.writerow( ('Topics', '#Occurrences') )
        for topic in sorted_topics:
            if count == limit:
                break
            print ('%s: %d occurrences'%(topic[0], topic[1]))
            writer.writerow([topic[0], topic[1]])
            count += 1

    def topicsByMonth(self, limit):
        first_date, last_date = self.get_first_last_date()

        d1 = date(first_date.year, first_date.month, first_date.day)
        d2 = date(last_date.year, last_date.month, last_date.day)
        months = self.diff_month(d2, d1)

        count = 1
        month_dicts = {}
        for i in range(months+ 1):
            month_dict = {}
            curr_date = d1 + relativedelta(months=+i)
            #print curr_date
            begin_date = datetime(curr_date.year,curr_date.month,curr_date.day, 00, 00, 00) + relativedelta(day=1)
            end_date = datetime(curr_date.year,curr_date.month,curr_date.day, 23, 59, 59) + relativedelta(day=1, months=+1, days=-1)
            #print begin_date  
            #print end_date
            res = self.mongo.fetch('feature-collection-mod', { 'created_at' : { "$gte": begin_date, "$lte": end_date}}, False)
            for record in res:
                if record['topic_words']:
                    topic_words = record['topic_words'][0].split()
                    for topic in topic_words:
                        if topic in month_dict:
                            month_dict[topic] += 1
                        else:
                            month_dict[topic] = 1

            sorted_topics = sorted(month_dict.iteritems(), key=operator.itemgetter(1), reverse = True)
            count = 1

            writer = csv.writer(open('topics_by_month_%d.csv'%i, 'wb'), delimiter=',', quotechar='"')
            writer.writerow( ('Topics', '#Occurrences') )
            for topic in sorted_topics:
                if count == limit:
                    break
                print ('%s: %d occurrences'%(topic[0], topic[1]))
                writer.writerow([topic[0], topic[1]])
                count += 1

            print

    #calendar.monthrange(year, month)[1]

    def topicsByWeekvsEnd(self, limit):
        week_days = ['mon', 'tue', 'wed', 'thu', 'fri']
        weekend_days = ['sat', 'sun']

        weekvsend_dict = {}
        weekend = 'weekend'
        week = 'week'

        res = self.mongo.fetch('feature-collection-mod', {}, False)

        for record in res:
            #curr_date = d1 + td(days=i)
            #begin_date = datetime(curr_date.year,curr_date.month,curr_date.day, 00, 00, 00)
            #end_date = datetime(curr_date.year,curr_date.month,curr_date.day, 23, 59, 59)
            curr_date = record['created_at']
            dayt = curr_date.strftime('%a').lower()

            if record['topic_words']:
                topic_words = record['topic_words'][0].split()
                if dayt in week_days:
                    if week not in weekvsend_dict:
                        weekvsend_dict[week] = {}
                    for topic in topic_words:
                        if topic in weekvsend_dict[week]:
                            weekvsend_dict[week][topic] += 1
                        else:
                            weekvsend_dict[week][topic] = 1

                elif dayt in weekend_days:
                    if weekend not in weekvsend_dict:
                        weekvsend_dict[weekend] = {}
                    for topic in topic_words:
                        if topic in weekvsend_dict[weekend]:
                            weekvsend_dict[weekend][topic] += 1
                        else:
                            weekvsend_dict[weekend][topic] = 1

        sorted_week_topics = sorted(weekvsend_dict[week].iteritems(), key=operator.itemgetter(1), reverse = True)
        writer = csv.writer(open('topics_by_weekdays.csv', 'wb'), delimiter=',', quotechar='"')
        writer.writerow( ('Topics', '#Occurrences') )
        count = 1
        for topic in sorted_week_topics:
            if count == limit:
                break
            print ('%s: %d occurrences'%(topic[0], topic[1]))
            writer.writerow([topic[0], topic[1]])
            count += 1

        print

        sorted_weekend_topics = sorted(weekvsend_dict[weekend].iteritems(), key=operator.itemgetter(1), reverse = True)
        writer = csv.writer(open('topics_by_weekend.csv', 'wb'), delimiter=',', quotechar='"')
        writer.writerow( ('Topics', '#Occurrences') )
        count = 1
        for topic in sorted_weekend_topics:
            if count == limit:
                break
            print ('%s: %d occurrences'%(topic[0], topic[1]))
            writer.writerow([topic[0], topic[1]])
            count += 1

    def countTopics(self, topic_words, cdict):
        for topic in topic_words:
            if topic in cdict:
                cdict[topic] += 1
            else:
                cdict[topic] = 1
        return cdict
 
    def topicsByDaytime(self, limit):
        res = self.mongo.fetch('feature-collection', {}, False)
        daytime_dict = {}
        daytime_dict['morning'] = {}
        daytime_dict['noon']  = {}
        daytime_dict['afternoon'] = {}
        daytime_dict['evening'] = {}
        daytime_dict['night'] = {}

        for record in res:
            if record['topic_words']:
                topic_words = record['topic_words'][0].split()
                date = record['created_at']
                h = date.hour
                if h < 4 or h >= 23:
                    daytime_dict['night'] = self.countTopics(topic_words, daytime_dict['night'])
                elif h >= 4 and h < 11:
                    daytime_dict['morning'] = self.countTopics(topic_words, daytime_dict['morning'])
                elif h >= 11 and h < 14:
                    daytime_dict['noon'] = self.countTopics(topic_words, daytime_dict['noon'])
                elif h >= 14 and h < 18:
                    daytime_dict['afternoon'] = self.countTopics(topic_words, daytime_dict['afternoon'])
                elif h >= 18 and h < 23:
                    daytime_dict['evening'] = self.countTopics(topic_words, daytime_dict['evening'])

        for k,v in daytime_dict.iteritems():
            writer = csv.writer(open('topics_by_%s.csv'%k, 'wb'), delimiter=',', quotechar='"')
            writer.writerow( ('Topics', '#Occurrences') )
            sorted_topics = sorted(v.iteritems(), key=operator.itemgetter(1), reverse = True)
            count = 1
            for topic in sorted_topics:
                if count == limit:
                    break
                print ('%s: %d occurrences'%(topic[0], topic[1]))
                writer.writerow([topic[0], topic[1]])
                count += 1
            print

def main(query, limit):
    limit = int(limit)
    analyzer = TopicAnalyzer()
    print query
    if query == 'whole': 
        analyzer.topicsByTotalPeriod(limit)
    elif query == 'month':
        analyzer.topicsByMonth(limit)
    elif query == 'week':
        analyzer.topicsByWeekvsEnd(limit)
    elif query == 'day':
        analyzer.topicsByDaytime(limit)

if __name__ == "__main__":   
    check = ['whole', 'month', 'week', 'day']
    if len(sys.argv) > 1 and sys.argv[1] in check:
        main(*sys.argv[1:])
    else:
        print "usage: %s (whole|month|week|day) limit"%(sys.argv[0])

