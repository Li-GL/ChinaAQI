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
	now_time = '1900/00/00 00:00:00'
	stop = 0
	time_compensate = 0
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

		#读取PM25in Json文件
		try:
			url = 'http://www.pm25.in/api/querys/all_cities.json?token=5j1znBVAsnSf5xQyNQyq'
			resp = urllib2.urlopen(url)
			data = json.loads(resp.read())
			#如果第一次请求失败尝试5min后再次请求
			if len(data)<100 and stop == 0:
				print 'Sorry, API error.', 'Try again in 5 min. ', now_time
				stop = 1
				time_compensate = 300
				time.sleep(300)
				continue
			#如果第二次还失败的话就加上刚才的时间等1小时
			if len(data)<100 and stop == 1:
				print 'Sorry, API is not available in this hour.', (' Wait for %s seconds' % time_check), now_time
				##记录丢失的数据
				with open('missingDataLog.txt', 'a') as f:
					missingTime = datetime.datetime.strptime(now_time, '%Y/%m/%d %H:%M:%S') - datetime.timedelta(hours=1)
					missingTime = missingTime.strftime('%Y/%m/%d %H:00:00')
					f.write(str(missingTime) + '\n')
				stop = 0
				time.sleep(time_check -299)
				continue

		# Pandas dataframe 处理
			column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co',u'no2', u'o3', u'so2', u'o3_8h', u'pm2_5_24h', u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h',  u'o3_8h_24h', u'aqi', u'quality',  u'primary_pollutant', u'station_code',u'province']
			frame = pd.DataFrame(data,columns=column)
			modeTime = frame['time_point'].mode()[0]
		except:
			print 'Sorry, some error occurred.', 'Request again in 20 seconds'
			time.sleep(20)
			continue
		#改下时间格式
		stop = 0
		DateTime = dateutil.parser.parse(modeTime)
		DateTime = DateTime.strftime('%Y/%m/%d %H:%M:%S')
		frame['time_point'] = DateTime

		#如果时间没发生变化就过一段时间继续check
		if DateTime == lastTime:
			print 'No change for the latest check at', now_time+'.', ('Wait for %s seconds' % time_check)
			processTime = time.time() - start_time
			time.sleep(time_check-processTime-time_compensate)
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

		processTime = time.time() - start_time
		print DateTime, '   ', ("Processed in  %.2f seconds at" % processTime), now_time

		time.sleep(time_check-processTime-time_compensate)

		time_compensate = 0
#######################################  Main Body #############################################
if __name__ == "__main__":

	engine = create_engine('', encoding='utf-8')

	with open('city_province.json') as f:
		string = f.read()
	provinces = json.loads(string)

	time_check = 3600
	dataToSQL()

