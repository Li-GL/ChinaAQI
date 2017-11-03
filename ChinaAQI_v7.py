#-*- coding:utf-8 -*-
import datetime
import dateutil.parser
import json
from sqlalchemy import create_engine
import pandas as pd
import urllib2
import time
import codecs

####每次check拿到对应的省份
def getProvince(City):
	for province in provinces:
		for city in province[u'children']:
			if City == city[u'text']:
				return province[u'text']
			elif City in province[u'text']:
				return province[u'text']

#####记录丢失数据的时间
def recordLoss(missingTime):
	with open('missingDataLog.txt', 'a') as f:
		f.write(str(missingTime) + '\n')

####补发NULL数据到数据库，使时间是连续的
def getNullDf(dateTime):
	data = pd.read_json(codecs.open('null.json', 'r', 'utf-8'))
	column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co', u'no2', u'o3', u'so2', u'o3_8h',
			  u'pm2_5_24h', u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h', u'o3_8h_24h', u'aqi', u'quality',
			  u'primary_pollutant', u'station_code', u'province']
	df = pd.DataFrame(data, columns=column)
	df['time_point'] = dateTime
	return df

####AQI数据处理
def getAQIDf(data):

	column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co', u'no2', u'o3', u'so2',
			  u'o3_8h', u'pm2_5_24h', u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h', u'o3_8h_24h',
			  u'aqi', u'quality', u'primary_pollutant', u'station_code', u'province']
	frame = pd.DataFrame(data, columns=column)
	modeTime = frame['time_point'].mode()[0]
	# 改下dataframe时间格式
	dateTime = dateutil.parser.parse(modeTime)
	dateTime = dateTime.strftime('%Y-%m-%d %H:%M:%S')
	frame['time_point'] = dateTime
	# 把对应省份加到dataframe
	allProvince = [getProvince(i) for i in frame['area']]
	provinceList = ['NA' if v is None else v for v in allProvince]
	frame['province'] = provinceList
	return dateTime, frame

####按照省份发往数据库
def toSQL(df,engine):
	# 要发往数据库的 31个省份
	with open('province.txt') as f:
		string = f.readlines()
		province_sql = [i.decode('utf-8').strip() for i in string]
	del province_sql[0]
	# 数据按照省份发往数据库
	for i in province_sql:
		t = df[df['province'].isin([i])]
		t.to_sql(i, con=engine, if_exists='append', chunksize=5000, index=False)

#######################################  主函数  #########################################
def mainFn():

	lastTime,nowTime = '','1900-00-00 00:00'
	fail,timeError,timeServerError = 0,0,0

	while True:

		#计算处理时间
		startTime = time.time()

		#如果服务器死掉，run again
		try:
			engine = create_engine('mysql+mysqldb://AerosolRoot:Passw0rd@139.159.221.133/chinaaqi?charset=utf8',encoding='utf-8')
		except:
			print 'Server may not be available, try again in 1 min'
			time.sleep(60)
			timeServerError = time.time() - startTime
			continue

		#读取过去最新的时间,没有数据库just pass
		try:
			nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			qur_lastTime = "SELECT * FROM  chinaaqi.上海 order by time_point DESC"
			allData = pd.read_sql(qur_lastTime, con=engine)
			lastTime = str(allData['time_point'][0])
		except:
			pass
			time.sleep(1)
			print 'query time error'

		#读取PM25in Json文件，遇到未知错误20秒后继续请求
		try:
			url = 'http://www.pm25.in/api/querys/all_cities.json?token=5j1znBVAsnSf5xQyNQyq'
			resp = urllib2.urlopen(url)
			data = json.loads(resp.read())

			#前两次请求失败尝试2min后再次请求， 第三次还失败的话就加上刚才的时间等1小时
			if len(data) < 100 and fail == 0:
				print 'Sorry, API error.', 'Try again in 2 min. ', nowTime
				fail = fail + 1
				time.sleep(120)
				continue
			if len(data) < 100 and fail == 1:
				print 'Sorry, API error.', 'Try twice in 2 min. ', nowTime
				fail = fail + 1
				time.sleep(120)
				continue
			if len(data) < 100 and fail == 2:
				print 'Sorry, API is not available in this hour.', (' Wait for %s seconds' % timeCheck), nowTime
				fail = 0
				time.sleep(timeCheck)
				continue
		except:
			print 'Sorry, some error occurred.', 'Request again in 20 seconds'
			time.sleep(20)
			timeError = time.time() - startTime + timeError
			continue

		try:
			#AQI数据处理
			dateTime, frame = getAQIDf(data)

			#如果数据没有变化就不发数据库，等待一段时间继续check
			lastTime = datetime.datetime.strptime(lastTime, '%Y-%m-%d %H:%M:%S')
			dateTime = datetime.datetime.strptime(dateTime, '%Y-%m-%d %H:%M:%S')
			hours = (dateTime - lastTime).seconds / 3600 + (dateTime - lastTime).days*24
			if hours==0:
				print 'No change for the latest check at', nowTime+'.', ('Wait for %s seconds' % timeCheck)
				processTime = time.time() - startTime
				time.sleep(timeCheck-processTime)
				continue
			elif hours>=2:
				for i in range(1, hours):
					compensateTime = lastTime + datetime.timedelta(hours=i)
					frameNull = getNullDf(str(compensateTime))
					toSQL(frameNull, engine)
					recordLoss(str(compensateTime))
					print 'Sending null data for missing hour    ', compensateTime
			#按照省份发往数据库
			toSQL(frame, engine)

			#总共的处理时间
			processTime = time.time() - startTime
			print dateTime, '   ', ("Processed in  %.2f seconds at" % processTime), nowTime
			
			#严格等待下一个时间间隔
			sleepTime = timeCheck-processTime-timeError-timeServerError
			if sleepTime>0:
				time.sleep(sleepTime)
			else:
				time.sleep(timeCheck)

			#参数归0
			fail,timeError,timeServerError = 0,0,0
		except:
			print 'some error...'
			time.sleep(20)
			continue

#######################################  Run #############################################
if __name__ == "__main__":

	#先打开放到内存里即可
	with open('city_province.json') as f:
		string = f.read()
	provinces = json.loads(string)
	#每小时check下数据
	timeCheck = 3600

	mainFn()