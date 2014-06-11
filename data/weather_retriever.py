import urllib2
import json
from datetime import date, timedelta as td

from mongo_retriever import mongoDB

from time import sleep
from pytz import *
EAT = timezone('Africa/Nairobi')

API_KEY = "b6d8d38436a81653"

def check_clock(last_time):
    elapsed_time = (datetime.now() - last_time).seconds
    if elapsed_time < RATE_LIMIT_WINDOW:
        write_log('Sleeping for %s seconds...\n' % (RATE_LIMIT_WINDOW - elapsed_time))
        sleep(RATE_LIMIT_WINDOW - elapsed_time)

def main():

    db = mongoDB()
    last_date = db.fetch_sorted_limited('tweet-collection','created_at', 'DESC', 1)[0]
    last_date = EAT.localize(last_date['created_at'])
    lyear = last_date.year
    lmonth = last_date.strftime('%m')
    lday = last_date.strftime('%d')

    first_date = db.fetch_sorted_limited('tweet-collection','created_at', 'ASC', 1)[0]

    first_date = EAT.localize(first_date['created_at'])
    fyear = first_date.year
    fmonth = first_date.strftime('%m')
    fday = first_date.strftime('%d')

    d1 = date(fyear,int(fmonth),int(fday))
    d2 = date(lyear,int(lmonth),int(lday))
    delta = d2 - d1

    count = 1
    for i in range(delta.days + 1):
        daydict = {}
        mydate = d1 + td(days=i)
        print mydate
        query = 'http://api.wunderground.com/api/%s/history_%s%s%s/q/Kenya/Nairobi.json'%(API_KEY,mydate.strftime("%Y"),mydate.strftime("%m"),mydate.strftime("%d"))
        f = urllib2.urlopen(query)
        json_string = f.read()
        parsed_json = json.loads(json_string)

        daydict['date'] = {}
        daydict['date']['day'] = mydate.strftime("%d")
        daydict['date']['month'] = mydate.strftime("%m")
        daydict['date']['year'] = mydate.strftime("%Y")

        daydict['query'] = query
        daydict['data'] = {}
        
        day_history = parsed_json['history']
        day_observations = day_history['observations']
        temp_list = []
        for obs_dict in day_observations:
            temp = int(float(obs_dict["tempm"]))
            if temp < 0:
                temp_list.append('N/A')
            else:
                temp_list.append(temp)

        print temp_list
        daydict['data']['temp'] = temp_list

        if day_history['dailysummary']:
            day_summary = day_history['dailysummary'][0]
            daydict['data']['rain'] = day_summary['rain']
            daydict['data']['precipi'] = day_summary['precipi']
            daydict['data']['humidity'] = day_summary['humidity']
            daydict['data']['meanpressurem'] = day_summary['meanpressurem']
            daydict['data']['meantemp'] = day_summary['meantempm']
            daydict['data']['mintemp'] = day_summary['mintempm']
            daydict['data']['maxtemp'] = day_summary['maxtempm']
            

        db.insert('weather-collection', daydict)
        f.close()

        if count % 9 == 0:
            sleep(66)
        count +=1


if __name__ == "__main__":
    main()