#-*- coding:utf-8 -*-
import suds
import json
import pandas as pd
import datetime, dateutil.parser
from sqlalchemy import create_engine
import time
def getQilian(datetime):

	wsdlFile = 'http://125.72.26.66:41553/service/ws/jsonWebService?wsdl'
	server = suds.client.Client(wsdlFile)
	params = {"pager":{"page":"1","pageSize":"10"},"order":[{"orderColumn":"","mode":""}],"params":[
	{"FieldName":"WQTIME","Value":datetime,"Operator":"<="},
	{"FieldName":"WQTIME","Value":datetime,"Operator":">="},{"FieldName":"POINTCODE","Value":"632200052","Operator":"="}]}

	interfaceId ='402882345c0eab9c015c34be74e00d10'
	token = 'common-interface'
	params = json.dumps(params)
	data = server.service.RunJsonResult(interfaceId,token,params)
	data = json.loads(data)
	return data

def data2sql(data):

	data = data[u'data']
	dataDict = data[0]

	col_qilian = [u'WQTIME', u'REGIONNAME',u'POINTNAME', u'XKL', u'KLW', u'YY', u'EYL',u'CY', u'EYD']
	data_q = [dataDict[i] for i in col_qilian]
	for i in range(1,13):
		data_q.append(None)
		i=i+1

	#按这个顺序排列
	column = [u'time_point', u'area', u'position_name', u'pm2_5', u'pm10', u'co',u'no2', u'o3', u'so2', u'o3_8h', u'pm2_5_24h',
			  u'pm10_24h', u'co_24h', u'no2_24h', u'o3_24h', u'so2_24h',  u'o3_8h_24h', u'aqi', u'quality',  u'primary_pollutant', u'station_code']

	frame = pd.DataFrame(data=[data_q], columns=column)
	timeWSDL = frame['time_point'][0]
	DateTime = dateutil.parser.parse(timeWSDL)
	DateTime = DateTime.strftime('%Y/%m/%d %H:%M:%S')
	frame['time_point'] = DateTime

	#发送数据到数据库
	frame.to_sql(u'青海省', con=engine, if_exists='append', chunksize=5000, index=False)


while True:
	try:

		#  初始化数据库连接:
		engine = create_engine('', encoding='utf-8')
		try:
			qur_lastTime = "SELECT * FROM  chinaaqi.青海省 where position_name='祁连县' order by time_point DESC"
			alldata = pd.read_sql(qur_lastTime, con=engine)
			lastTime = alldata['time_point'][0]
			lastTime = dateutil.parser.parse(lastTime)
			lastTime = lastTime.strftime('%Y-%m-%d %H:00:00')
		except:
			lastTime = ''

		request_time = datetime.datetime.now()- datetime.timedelta(hours=1)
		request_time = request_time.strftime('%Y-%m-%d %H:00:00')
		request_time = '2017-07-06 09:00:00'
		if request_time == lastTime:
			print 'No change for the latest check'
			time.sleep(1200)
			continue

		data = getQilian(request_time)
		data2sql(data)

		print request_time, "Processed"
		time.sleep(1200)

	except:
		print 'No data, request later'
		time.sleep(1200)
