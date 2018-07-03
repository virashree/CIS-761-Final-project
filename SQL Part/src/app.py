import psycopg2
import math

class Functions:

    def __init__(self):
        pass

    def topHashtags(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("select count(h.hashtag) as tophashtag, h.hashtag "
                        "from hashtags h join contain c on h.hashtag = c.hashtag "
                        "group by h.hashtag "
                        "order by tophashtag desc "
                        "Limit 5")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def KCtweets(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("select distinct t.*"
                        "from tweets t join contain c on t.t_id = c.t_id"
	                     "join hashtags h on h.hashtag = c.hashtag"
                        "where (place like '%MO' or place like '%KS%')")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def topUsers(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("select count(p.t_id) as cnt, u.* "
                        "from users u join post p on p.u_id = u.u_id "
                        "join tweets t on p.t_id = t.t_id "
                        "group by u.u_id "
                        "order by cnt desc "
                        "limit 5")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def noOfHotDays(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(maxtemp) as hotdays, EXTRACT(YEAR FROM date_) as year "
                        "from kansascity "
                        "WHERE maxtemp >= 80"
                        "GROUP BY year "
                        "ORDER BY year desc")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def minTemp(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT min(WinterTempByday.mintemp) as temp, EXTRACT(YEAR FROM date_) as year "
                        "FROM (SELECT mintemp, date_ FROM kansascity "
                        "WHERE (EXTRACT(MONTH FROM date_) = 12 OR (EXTRACT(MONTH FROM date_) >= 1 "
                        "AND EXTRACT(MONTH FROM date_) <=3 )) and mintemp is not NULL) WinterTempByday "
                        "GROUP BY year "
                        "ORDER BY year desc")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def maxTemp(self):

        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT  max(SummerTempByday.maxtemp) as temp, EXTRACT(YEAR FROM date_) as year FROM "
                        "(SELECT maxtemp, date_ "
                        "FROM kansascity "
                        "WHERE (EXTRACT(MONTH FROM date_) >= 4 AND EXTRACT(MONTH FROM date_) <=11 ) "
                        "and maxtemp is not NULL) SummerTempByday "
                        "GROUP BY year "
                        "ORDER BY year desc")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def lastYearByMonth(self):
        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT min(mintemp),max(maxtemp),extract(month from date_) as month "
                        "from kansascity "
                        "where extract(year from date_) = 2016 "
                        "group by month "
                        "order by month")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def sixtyYearsBeforeByMonth(self):
        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT min(mintemp),max(maxtemp),extract(month from date_) as month "
                        "from kansascity "
                        "where extract(year from date_) = 1946 "
                        "group by month "
                        "order by month")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

    def dayOverSeventy(self):
        conn = psycopg2.connect("dbname='Vira' user='postgres' host='localhost' password='Biladi13'")
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(maxtemp) as hotdays, EXTRACT(YEAR FROM date_) as year "
                        "from kansascity "
                        "WHERE maxtemp >= 70"
                        "GROUP BY year "
                        "ORDER BY year desc")
        except:
            print("something is wrong")
        rows = cur.fetchall()
        cur.close()
        return rows

if __name__ == '__main__':
    q = Functions()






