from tkinter import *
from tkinter.messagebox import *
from prettytable import PrettyTable
from src import app
import math
from highcharts import Highchart

print("\n")
print("***********************************************************************************************************")
print("***********************************************************************************************************")
print("***********************************************************************************************************")
print("\n")
print('Welcome to TweetWeather!')
print("Please enter the query number you would like to get the data for from the following options")
options = ["Quit","top 5 Hashtag", "Tweets From Kansas City Area", "Top 5 Users", "Max and Min temp for year 2016",
           "Number of hot days by year", "Minimum temp for winter by year", "Maximum temp for summer by year", "Number Of Days Over 70°F by year"]
print("\n")
cnt = 0
for i in options:
    print(cnt,":", i)
    cnt = cnt + 1
print("\n")

query = 1
q = app.Functions()
try:
    while query != 0:
        query = input("Query: ")
        query = int(query)

        if query == 1:
            data = q.topHashtags()
            plot = []
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Hashtag", "Count"]
                for row in data:
                    hashtag = row[1]
                    count = row[0]
                    table.add_row([hashtag, count])
                    plot.append([hashtag,count])
                print(table)

            chart = Highchart(width=664, height=400)

            chart.set_options('chart', {'inverted': False})

            options = {
                'chart': {
                    'type': 'pie',
                    'options3d': {
                        'enabled': True,
                        'alpha': 45
                    }
                },
                'title': {
                    'text': 'Top Hashtags'
                },
                'subtitle': {
                    'text': 'Observed in Kansas City, Kansas, USA'
                },
                'plotOptions': {
                    'pie': {
                        'innerSize': 100,
                        'depth': 45
                    }
                }
            }

            chart.set_dict_options(options)

            data = plot
            chart.add_data_set(data, 'pie' , 'Count')

            chart.save_file("hashtags")


        elif query == 2:
            data = q.KCtweets()
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Tweet Id ", "Place", "Create at", "Favorites", "Content"]
                for row in data:
                    t_id = row[0]
                    place = row[1]
                    created_at = row[2]
                    fav = row[3]
                    content = row[4]
                    table.add_row([t_id, place, created_at, fav, content])
                print(table)

        elif query == 3:
            data = q.topUsers()
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Tweet Count ", "User Id", "Name", "Screen Name", "Location", "Description", "TimeZone",
                                     "Friends Count", "Favorites count", "Followers Count", "Status Count"]

                for row in data:
                    table.add_row([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]])
                print(table)

        elif query == 4:

            data = q.lastYearByMonth()
            temp = []
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                for row in data:
                    mintemp = float(row[0])
                    maxtemp = float(row[1])
                    temp.append([mintemp, maxtemp])

            chart = Highchart(width=664, height=400)

            chart.set_options('chart', {'inverted': True})

            options = {
                'title': {
                    'text': 'Temperature variation by month'
                },
                'subtitle': {
                    'text': 'Observed in Kansas City, Kansas, USA - 2016'
                },
                'xAxis': {
                    'categories': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                    'reversed': False,
                    'title': {
                        'enabled': True,
                        'text': 'Month'
                    },
                    'maxPadding': 0.05,
                    'showLastLabel': True
                },
                'yAxis': {
                    'title': {
                        'text': 'Temperature ( °F ) '
                    },
                    'lineWidth': 2
                },
                'legend': {
                    'enabled': False
                },
                'tooltip': {
                    'valueSuffix': '°F'
                }
            }

            chart.set_dict_options(options)

            print(temp)
            data = temp
            chart.add_data_set(data, 'columnrange', 'Temperature', marker={'enabled': False})
            chart.save_file("year2016")

        elif query == 5:
            data = q.noOfHotDays()
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Year", "Number of Hot Days"]
                for row in data:
                    year = int(row[1])
                    cnt = row[0]
                    table.add_row([year,cnt])
                print(table)

        elif query == 6:
            data = q.minTemp()
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Year", "Lowest Temperature"]
                for row in data:
                    year = int(row[1])
                    temp = row[0]
                    table.add_row([year, temp])
                print(table)

        elif query == 7:
            data = q.maxTemp()
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Year", "Highest Temperature"]
                for row in data:
                    year = int(row[1])
                    temp = row[0]
                    table.add_row([year, temp])
                print(table)

        elif query == 8:
            data = q.dayOverSeventy()
            days = []
            years = []
            plot = []
            if data is None:
                print("Sorry, No Results Are Found!")
            else:
                table = PrettyTable()
                table.field_names = ["Year", "Number of Hot Days"]
                for row in data:
                    year = int(row[1])
                    cnt = int(row[0])
                    table.add_row([year, cnt])
                    days.append(cnt)
                    years.append(year)
                #print(table)
            count = 0
            for y,d in zip(years,days):
                if count%5 == 0:
                    plot.append([y,d])
                elif d >= 200:
                    plot.append([y,d])
                count = count + 1

            for i in plot:
                if i[1] == 0:
                    plot.remove(i)

            chart = Highchart(width=1220)

            chart.set_options('chart', {'inverted': False})

            options = {
                'title': {
                    'text': 'Number of days over 70 per year'
                },
                'subtitle': {
                    'text': 'Observed in Kansas City, Kansas, USA : 1934 - 2016'
                },
                'xAxis': {
                    'max': 2017,
                    'min': 1934,
                    'labels': {
                        'rotation': -45,
                            'style': {
                                'fontSize': '13px',
                                'fontFamily': 'Verdana, sans-serif'
                        }
                    },
                    'reversed': False,
                    'title': {
                        'enabled': True,
                        'text': 'Year'
                    },
                    'maxPadding': 0.05,
                    'showLastLabel': True
                },
                'yAxis': {
                    'title': {
                        'text': 'Number of days ( °F ) '
                    },
                    'lineWidth': 2
                },
                'legend': {
                    'enabled': False
                },
                'tooltip': {
                    'pointFormat': "Number of days over 70°: <b>{point.y:.1f} </b>"
                }
            }

            chart.set_dict_options(options)
            data = plot
            chart.add_data_set(data, 'column', marker={'enabled': False})

            chart.save_file("DaysOver70")

    else:
        print("Thank you for using TweetWeather! Good bye!")
        exit(0)

except KeyboardInterrupt:
    print("Exit")





