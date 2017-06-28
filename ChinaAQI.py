#-*- coding:utf-8 -*-
import datetime, dateutil.parser
import json
from sqlalchemy import create_engine
import pandas as pd
import urllib2
import time

#######################################  函数部分 #############################################

####每次check拿到对应的省份
def getProvince(City):
	for province in provinces:
		for city in province[u'children']:
			if City == city[u'text']:
				return province[u'text']
			elif City in province[u'text']:
				return province[u'text']

####数据到数据库
def dataToSQL():
	lastTime = ''
	while True:

		start_time = time.time()

		#读取过去最新的时间,没有数据库就默认时间为空
		try:
			now_time = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
			qur_lastTime = "SELECT * FROM  chinaaqi.北京 where time_point between '%s' and '%s' order by time_point DESC" % (lastTime.encode('utf-8'), now_time)
			alldata = pd.read_sql(qur_lastTime, con=engine)
			lastTime = alldata['time_point'][0]
		except:
			lastTime = ''

		#读取Json文件，注意这个是公测的API key
		url = 'http://www.pm25.in/api/querys/all_cities.json?token=5j1znBVAsnSf5xQyNQyq'
		resp = urllib2.urlopen(url)
		data = json.loads(resp.read())
		if 'error' in data:
			print 'Sorry, API is not available in this hour.  '
			print 'Try wait for %s seconds' % time_check
			time.sleep(time_check)
			continue

		# Pandas dataframe 处理
		column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co',u'no2', u'o3', u'so2', u'o3_8h', u'pm2_5_24h', u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h',  u'o3_8h_24h', u'aqi', u'quality',  u'primary_pollutant', u'station_code',u'province']
		frame = pd.DataFrame(data,columns=column)
		modeTime = frame['time_point'].mode()[0]

		#改下时间格式
		DateTime = dateutil.parser.parse(modeTime)
		DateTime = DateTime.strftime('%Y/%m/%d %H:%M:%S')
		frame['time_point'] = DateTime
		#如果时间没发生变化就过一段时间继续check
		if DateTime == lastTime:
			time.sleep(time_check)
			print 'No change for the latest check'
			continue

		#把对应省份加到dataframe
		allProvince = [getProvince(i) for i in frame['area']]
		provinceList = ['NA' if v is None else v for v in allProvince]
		frame['province'] = provinceList

		#要发往数据库的 31个省份
		with open('province.txt') as f:
			string = f.readlines()
			province_sql = [i.decode('utf-8').strip() for i in string]
		del province_sql[0]

		# 数据按照省份发往数据库
		for i in province_sql:
			t = frame[frame['province'].isin([i])]
			t.to_sql(i, con=engine, if_exists='append', chunksize=5000, index=False)

		print DateTime, '   ', ("Processed in  %.2f seconds" % (time.time() - start_time))

		time.sleep(time_check)

#######################################  Main Body #############################################
if __name__ == "__main__":

	#注意改数据库哦,还有上面的API key,和对应的北京那张表的数据库名
	engine = create_engine('mysql+mysqldb://root:lgl521521@127.0.0.1:3306/test?charset=utf8', encoding='utf-8')

	with open('city_province.json') as f:
		string = f.read()
	provinces = json.loads(string)

	time_check = 3660
	dataToSQL()

