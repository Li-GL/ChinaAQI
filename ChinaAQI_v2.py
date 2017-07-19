#-*- coding:utf-8 -*-
import datetime
import dateutil.parser
import json
from sqlalchemy import create_engine
import pandas as pd
import urllib2
import time

#######################################  Function  #############################################

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
	now_time = '1900-00-00 00:00'
	fail = 0
	time_error = 0
	time_severerror = 0

	while True:

		#计算处理时间
		start_time = time.time()

		#如果服务器死掉，run again
		try:
			engine = create_engine('mysql+mysqldb://AerosolRoot:Passw0rd@139.159.221.133/chinaaqi?charset=utf8',encoding='utf-8')
		except:
			print 'Server may not be available, try again in 1 min'
			time.sleep(60)
			time_severerror = time.time() - start_time
			continue

		#读取过去最新的时间,没有数据库just pass
		try:
			now_time = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
			qur_lastTime = "SELECT * FROM  chinaaqi.北京 where time_point between '%s' and '%s' order by time_point DESC" % (lastTime.encode('utf-8'), now_time)
			alldata = pd.read_sql(qur_lastTime, con=engine)
			lastTime = alldata['time_point'][0]
		except:
			pass

		#读取PM25in Json文件，遇到未知错误20秒后继续请求
		try:
			url = 'http://www.pm25.in/api/querys/all_cities.json?token=5j1znBVAsnSf5xQyNQyq'
			resp = urllib2.urlopen(url)
			data = json.loads(resp.read())
			#如果第一次请求失败尝试5min后再次请求
			if len(data)<100 and fail == 0:
				print 'Sorry, API error.', 'Try again in 2 min. ', now_time
				fail = fail + 1
				time.sleep(120)
				continue
			# 如果第二次请求失败尝试5min后再次请求
			if len(data) < 100 and fail == 1:
				print 'Sorry, API error.', 'Try twice in 2 min. ', now_time
				fail = fail + 1
				time.sleep(120)
				continue
			#如果第三次还失败的话就加上刚才的时间等1小时
			if len(data)<100 and fail == 2:
				print 'Sorry, API is not available in this hour.', (' Wait for %s seconds' % time_check), now_time
				##记录丢失的数据
				with open('missingDataLog.txt', 'a') as f:
					missingTime = datetime.datetime.strptime(now_time, '%Y/%m/%d %H:%M:%S') - datetime.timedelta(hours=1)
					missingTime = missingTime.strftime('%Y/%m/%d %H:00:00')
					f.write(str(missingTime) + '\n')
				fail = 0
				time.sleep(time_check)
				continue
		# Pandas dataframe 处理
			column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co',u'no2', u'o3', u'so2', u'o3_8h', u'pm2_5_24h', u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h',  u'o3_8h_24h', u'aqi', u'quality',  u'primary_pollutant', u'station_code',u'province']
			frame = pd.DataFrame(data,columns=column)
			modeTime = frame['time_point'].mode()[0]
		except:
			print 'Sorry, some error occurred.', 'Request again in 20 seconds'
			time.sleep(20)
			time_error = time.time() - start_time + time_error
			continue

		try:
			#改下dataframe时间格式
			DateTime = dateutil.parser.parse(modeTime)
			DateTime = DateTime.strftime('%Y/%m/%d %H:%M:%S')
			frame['time_point'] = DateTime
			#把对应省份加到dataframe
			allProvince = [getProvince(i) for i in frame['area']]
			provinceList = ['NA' if v is None else v for v in allProvince]
			frame['province'] = provinceList

			#如果数据没有变化就不发数据库，等待一段时间继续check
			if DateTime == lastTime:
				print 'No change for the latest check at', now_time+'.', ('Wait for %s seconds' % time_check)
				processTime = time.time() - start_time
				time.sleep(time_check-processTime)
				continue

			#要发往数据库的 31个省份
			with open('province.txt') as f:
				string = f.readlines()
				province_sql = [i.decode('utf-8').strip() for i in string]
			del province_sql[0]
			# 数据按照省份发往数据库
			for i in province_sql:
				t = frame[frame['province'].isin([i])]
				t.to_sql(i, con=engine, if_exists='append', chunksize=5000, index=False)

			#总共的处理时间，并print
			processTime = time.time() - start_time
			print DateTime, '   ', ("Processed in  %.2f seconds at" % processTime), now_time
			#严格等待下一个时间间隔
			time.sleep(time_check-processTime-time_error-time_severerror)

			#参数归0
			fail = 0
			time_error = 0
			time_severerror = 0
		except:
			print 'some error...'
			time.sleep(20)
			continue
#######################################  Main Body #############################################
if __name__ == "__main__":

	#先打开放到内存里即可
	with open('city_province.json') as f:
		string = f.read()
	provinces = json.loads(string)
	#每小时check下数据
	time_check = 3600
	dataToSQL()