#test comment for branching 2_15_16
#1. create database + get data
#dark sky forecast api - laurie11ucd@gmail.com, pythoninfo1
# my key: f6893c03438de427a142cb676313c091
#example https://api.forecast.io/forecast/f6893c03438de427a142cb676313c091/37.727, -123.032

import datetime
import requests
import time
import collections
import sqlite3 as lite

con = lite.connect('weather.db')
cur = con.cursor()

api_key = "f6893c03438de427a142cb676313c091"
api = 'https://api.forecast.io/forecast/' + api_key



cities = {"Denver": '39.761850,-104.881105',
"Las Vegas": '36.229214,-115.26008',
"New York": '40.663619,-73.938589',
"San Francisco": '37.727239,-123.032229',
"Seattle": '47.620499,-122.350876'
}
end_date = datetime.datetime.now()
start_date = datetime.datetime.now() - datetime.timedelta(days=30)

with con:
    cur.execute('CREATE TABLE daily_temp ( day_of_reading text, Denver real, "Las Vegas" real, "New York" real, "San Francisco" real, Seattle real);')

with con:
	while start_date < end_date:
		cur.execute("INSERT INTO daily_temp(day_of_reading) VALUES (?)", (start_date.strftime('%Y-%m-%dT%H:%M:%S'),)) #strftime returns a string/text
		start_date += datetime.timedelta(days=1)

for k,v in cities.iteritems():
	start_date = end_date - datetime.timedelta(days=30) #set value each time through the loop of cities
	while start_date < end_date:
        #query for the value
		r = requests.get(api + "/" + v + ',' +  start_date.strftime('%Y-%m-%dT%H:%M:%S'))
		stringprint=(api + "/" + v + ',' +  start_date.strftime('%Y-%m-%dT%H:%M:%S'))
		print stringprint
		with con:
            #insert the temperature max to the database
			blob = ('UPDATE daily_temp SET "' + k + '" = ' + str(r.json()['daily']['data'][0]['temperatureMax']) + ' WHERE day_of_reading = ' + start_date.strftime('%Y-%m-%dT%H:%M:%S'))
			print blob
			cur.execute('UPDATE daily_temp SET "' + k + '" = ' + str(r.json()['daily']['data'][0]['temperatureMax']) + ' WHERE day_of_reading = "' + start_date.strftime('%Y-%m-%dT%H:%M:%S') + '"')
			
        #increment start_date to the next day for next operation of loop
		start_date += datetime.timedelta(days=1) #increment start_date to the next day


con.close() # a good practice to close connection to database

#2. run analysis
import pandas as pd

con = lite.connect('weather.db')
cur = con.cursor()
rows = cur.fetchall()

df = pd.read_sql_query("SELECT * FROM daily_temp ORDER BY day_of_reading",con,index_col='day_of_reading')

max_change = collections.defaultdict(float)
for col in df.columns: #attribute of dataframe which is name of columns. always iterating over a list, returns whatever is in there (col). common thing is X range or list/dict
	#print df.columns
	temps = df[col].tolist()
	#print temps
	temp_change = 0
	for k,v in enumerate(temps): 
		if k < len(temps) - 1:
			if abs(temps[k] - temps[k+1]) > temp_change:
				temp_change = abs(temps[k] - temps[k+1])
			else:
				temp_change +=0
			#print temp_change
	max_change[col] = temp_change
#alternatively, could have found max and min of temps...rather than max change from day to day
	
for x, y in max_change.items():
	print ("The max day to day temperature change for " + x + " is "+ str(y)+".")	

yy = []
for x, y in max_change.items():
	yy.append(y)
	
plt.hist(yy)
plt.title("Changes in day to day Temperature")
plt.xlabel("Value")
plt.ylabel("Frequency")
plt.show()
	