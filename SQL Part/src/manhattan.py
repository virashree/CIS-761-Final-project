import requests
import psycopg2
import json

# insert weather data from api
# getting avg temp
#unknown data 'M'
params = {"sid":"usc00144972","sdate":"2000-01-01","edate":"2017-03-10","elems":"avgt","meta":"name"}
request = requests.get("http://data.rcc-acis.org/StnData",params)
avgtemp_content = request.content
parsed_json_avg_temp = json.loads(avgtemp_content.decode('utf-8'))
#print(parsed_json_avg_temp['data'])

# getting snow
# unknown data 'T'
params = {"sid":"usc00144972","sdate":"2000-01-01","edate":"2017-03-10","elems":"snow","meta":"name"}
request = requests.get("http://data.rcc-acis.org/StnData",params)
snow_content = request.content
parsed_json_snow = json.loads(snow_content.decode('utf-8'))
#print(parsed_json_snow['data'])

#getting max temp
# unknown data 'M'
params = {"sid":"usc00144972","sdate":"2000-01-01","edate":"2017-03-10","elems":"maxt","meta":"name"}
request = requests.get("http://data.rcc-acis.org/StnData",params)
maxt_content = request.content
parsed_json_maxt = json.loads(maxt_content.decode('utf-8'))
#print(parsed_json_maxt['data'])

# getting min temp
# unknown data 'M'
params = {"sid":"usc00144972","sdate":"2000-01-01","edate":"2017-03-10","elems":"mint","meta":"name"}
request = requests.get("http://data.rcc-acis.org/StnData",params)
mint_content = request.content
parsed_json_mint = json.loads(mint_content.decode('utf-8'))
#print(parsed_json_mint['data'])

#getting avg rain
# unknown data in 'T'
params = {"sid":"usc00144972","sdate":"2000-01-01","edate":"2017-03-10","elems":"pcpn","meta":"name"}
request = requests.get("http://data.rcc-acis.org/StnData",params)
pcpn_content = request.content
parsed_json_pcpn = json.loads(pcpn_content.decode('utf-8'))
#print(parsed_json_pcpn['data'])

count = 0
date = []
temp = []
for tempOnDate in parsed_json_avg_temp['data']:
    for data in tempOnDate:
        if count == 0:
            date.append(data)
            count = count + 1
        elif count == 1:
            temp.append(data)
            count = 0

for n,i in enumerate(temp):
    if i == 'M':
        temp[n] = None

count = 0
snow = []
for snowOnDate in parsed_json_snow['data']:
    for data in snowOnDate:
        if count == 0:
            count = count + 1
        elif count == 1:
            snow.append(data)
            count = 0

for n,i in enumerate(snow):
    if i == 'T' or i == 'M':
        snow[n] = None

count = 0
maxt = []
for maxtOnDay in parsed_json_maxt['data']:
    for data in maxtOnDay:
        if count == 0:
            count = count + 1
        elif count == 1:
            maxt.append(data)
            count = 0

for n,i in enumerate(maxt):
    if i == 'M':
        maxt[n] = None

mint = []
for mintOnDay in parsed_json_mint['data']:
    for data in mintOnDay:
        if count == 0:
            count = count + 1
        elif count == 1:
            mint.append(data)
            count = 0

for n,i in enumerate(mint):
    if i == 'M':
        mint[n] = None

pcpn = []
for pcpnOnDay in parsed_json_pcpn['data']:
    for data in pcpnOnDay:
        if count == 0:
            count = count + 1
        elif count == 1:
            pcpn.append(data)
            count = 0

for n,i in enumerate(pcpn):
    if i == 'T' or i == 'M':
        pcpn[n] = None

#conn = psycopg2.connect("dbname='viru' user='viru' host='postgresql.cs.ksu.edu' password='Cisiscool@2017'")
conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
cur = conn.cursor()

if len(date) == len(temp) == len(snow) == len(mint) == len(maxt) == len(pcpn):
    for d,t,s,m,mt,p in zip(date, temp, snow, mint, maxt, pcpn):
        print(d,t,s,m,mt,p)
        insert_manhattan_sql = "INSERT INTO manhattan (date_,avgtemp,snow, maxtemp, mintemp, avgrain) VALUES(%s,%s,%s,%s,%s,%s);"
        data = (d,t,s,mt,m,p)
        cur.execute(insert_manhattan_sql, data)
        conn.commit()
else:
    raise AssertionError

# insert from table

# with open('GlobalTempAnamoliesJSON.json') as json_data:
#     globalAnamolies = json.load(json_data)
#
# for key,value in globalAnamolies['data'].items():
#     insertAvgTempSql = "INSERT INTO globalanamolies (year,anamolies) VALUES(%s,%s);"
#     data = (key,value)
#     cur.execute(insertAvgTempSql, data)
#     conn.commit()
