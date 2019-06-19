import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore, db
import pandas as pd
import schedule
import time
from pytrends.request import TrendReq
import pytz
from datetime import datetime
import pandas as pd
from pyspark import SparkContext
from operator import add
# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

cred = credentials.Certificate('./myjson/serviceAccountKey.json')

firebase_admin.initialize_app(cred)

db = firestore.client()

collect_name = 'GoogleTrend'

sc = SparkContext("local", "Accumulator app") 

old_keylist = []


### sequential sending
def job():
    global collect_name
    global pytrend
    global sc
    global old_keylist
    #pytrend = TrendReq()
    trending_searches_df = pytrend.trending_searches(pn='taiwan')
    l = trending_searches_df.values.tolist()
    keylist = [item for sublist in l for item in sublist]
    if len(old_keylist) == 0:
        old_keylist = keylist
    else:
        single = {}
        new = [s for s in keylist if s not in old_keylist]
        single['keyword'] = new
        #doc_ref = db.collection(collect_name).document('New')
        #doc_ref.set(single)
    print(keylist)
    assert len(keylist) == 20
    for i in range(4):
        # Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
        pytrend.build_payload(kw_list=keylist[5*i:5*(i+1)], cat=0, timeframe='now 1-H', geo='TW', gprop='')
        # Interest Over Time
        interest_over_time_df = pytrend.interest_over_time()
        print(interest_over_time_df.tail())
        index = interest_over_time_df.index
        tw = pytz.timezone('Asia/Taipei')
        try:
            interest_over_time_df.index = index.tz_localize(pytz.utc).tz_convert(tw).strftime('%Y-%m-%d %H:%M')
        except:
            print(index)
            print(type(index))
        time.sleep(20)
        #print(interest_over_time_df.tail())
        #for j in range(5):
            #single = {}
            #single['name'] = keylist[5*i+j]
            #single['time'] = interest_over_time_df.index.values.tolist()
            #single['click'] = interest_over_time_df[keylist[5*i+j]].values.tolist()
            
            #rdd = sc.parallelize(interest_over_time_df[keylist[5*i+j]].values.tolist())
            #sum_ = rdd.reduce(add)
            #single['value'] = sum_

            #single['value'] = sum(interest_over_time_df[keylist[5*i+j]])

            #doc_ref = db.collection(collect_name).document('Top {:d}'.format(5*i+j+1))
            #doc_ref.set(single)
    #print('-----Success-----')

#job()
#schedule.every(2).minutes.do(job)
while True:
    #schedule.run_pending()
    job()
    time.sleep(1)
