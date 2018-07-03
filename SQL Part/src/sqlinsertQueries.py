import requests
import psycopg2
import urllib.parse as urlparse
import json
import time
import datetime
import pandas as pd
import numpy as np

tweets_data = []
tweets_file = open('earth_day_1.json', "r")
date = []
text = []
users = []
entities = []
favorite_count = []
ids = []
place = []
count = 0
country = []
# filtering the tweets and extracting the data
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)

        if tweet['place'] != None:
            if tweet['place']['country'] == 'United States':
                place.append(tweet['place']['full_name'])
                ts = time.strftime('%Y-%m-%d %H:%M:%S',
                time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                year = int(ts[0:4])
                month = int(ts[5:7])
                day = int(ts[8:10])
                hour = int(ts[11:13])
                minute = int(ts[14:16])
                sec = int(ts[17:19])
                dt = datetime.datetime(year, month, day, hour, minute, sec)
                unix_timestamp = time.mktime(dt.timetuple())
                date.append(unix_timestamp)
                text.append(tweet['text'])
                users.append(tweet['user'])
                entities.append(tweet['entities'])
                favorite_count.append(tweet['favorite_count'])
                ids.append(tweet['id'])
        else:
            continue
    except:
        continue

# establishing connection with postgres
conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
cur = conn.cursor()

u_ids = []
names = []
screen_name = []
location = []
description = []
time_zone = []
friends_count = []
favourites_count = []
followers_count = []
status_count = []

for user in users:
    u_ids.append(user['id'])
    names.append(user['name'])
    screen_name.append(user['screen_name'])
    location.append(user['location'])
    description.append(user['description'])
    time_zone.append(user['time_zone'])
    friends_count.append(user['friends_count'])
    favourites_count.append(user['favourites_count'])
    followers_count.append(user['followers_count'])
    status_count.append(user['statuses_count'])

hashtags= []
htags = []
x = []
for entity in entities:
    temp = entity['hashtags']
    for t in temp:
        x.append(t['text'])
        htags.append(t['text'])
    hashtags.append(x)
    x = []

print(len(place))
print(len(users))
print(len(date))

#inserting data to tweets table
#
# if len(date) == len(text) == len(ids) == len(place) == len(favorite_count) :
#     try:
#         for d,t,i,p,f in zip(date,text,ids,place,favorite_count):
#             #print(i,p,d,f,t)
#             insert_tweets_sql = "INSERT INTO tweets (t_id,place,created_at,favorites,content,country) VALUES(%s,%s,%s,%s,%s);"
#             data = (i,p,d,f,t)
#             cur.execute(insert_tweets_sql, data)
#             conn.commit()
#     except psycopg2.IntegrityError:
#         conn.rollback()
#     else:
#         conn.commit()
# else:
#     raise AssertionError
#
# #
# # inserting data to users table
# if len(u_ids) == len(names) == len(screen_name) == len(location) == len(description) == \
#         len(time_zone) == len(friends_count) == len(favourites_count) == len(followers_count) == len(status_count):
#     try:
#         for u,n,sn,l,d,tz,frnc,favc,folc,sc in zip(u_ids,names,screen_name,location,description,time_zone,friends_count,favourites_count,followers_count,status_count):
#             insert_users_sql = "INSERT INTO users " \
#                                 "(u_id,name,screen_name,location,description,time_zone,friends_count,favourites_count,followers_count,status_count) " \
#                                 "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#             data = (u,n,sn,l,d,tz,frnc,favc,folc,sc)
#             cur.execute(insert_users_sql, data)
#             conn.commit()
#     except psycopg2.IntegrityError:
#         conn.rollback()
#
#
# else:
#     raise AssertionError
#
# print(len(ids))
# print(len(u_ids))
#
#
#
# #inserting data to hashtags table
# for htag in hashtags:
#     if htag is not []:
#         try:
#             for h in htag:
#                 insert_hashtag_sql = "INSERT INTO hashtags (hashtag) VALUES(%s);"
#                 data = (h,)
#                 cur.execute(insert_hashtag_sql, data)
#                 conn.commit()
#         except psycopg2.IntegrityError:
#             conn.rollback()
#         else:
#             conn.commit()
#     else:
#         continue


##inserting data to post table
# if len(u_ids) == len(ids):
#     try:
#         for u,t in zip(u_ids,ids):
#             insert_post_sql = "INSERT INTO post (u_id,t_id) VALUES(%s,%s);"
#             data = (u,t)
#             cur.execute(insert_post_sql, data)
#             conn.commit()
#     except psycopg2.IntegrityError:
#         conn.rollback()
#     else:
#         conn.commit()
# else:
#     raise AssertionError
#
# print(len(hashtags))
# print(len(ids))
#
# ## inserting data to contain table
# if len(hashtags) == len(ids):
#     try:
#         for htag,t in zip(hashtags,ids):
#             for h in htag:
#                 insert_contain_sql = "INSERT INTO contain (t_id,hashtag) VALUES(%s,%s);"
#                 data = (t,h)
#                 cur.execute(insert_contain_sql, data)
#                 conn.commit()
#     except psycopg2.IntegrityError:
#         conn.rollback()
#     else:
#         conn.commit()
# else:
#     raise AssertionError
#
