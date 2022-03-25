import requests, datetime

base_url = 'https://msn-api.seas-nve.dk/api/v1.0'
class SeasNveApi:
	def __init__(self, EMAIL, PASSWORD):
		global authheader
		try:
			resp = requests.post(base_url + '/auth', json={'username': EMAIL, 'password': PASSWORD})
			Bearer = 'Bearer '+ resp.json()['accessToken']
			authheader = {'Authorization': Bearer}
		except Exception as e:
			error = 1
			print(e)

	def getMeteringPoint(self, type):
		m = requests.get(base_url+'/profile/metering/', headers=authheader)
		data = m.json()[0]	# TODO: Search meteringpoints for meterType == type
		meteringPoint = data['meteringPoint']
		return meteringPoint

	def consumption(self,aggr,start,end):
		# Valid aggr is Hour, Day, Month, Year
		mpn = self.getMeteringPoint('Power')
		m = requests.get(base_url+'/profile/consumption/?meteringpoints='+mpn+'&start='+start+'&end='+end+'&aggr='+aggr+'', headers=authheader)
		data = m.json()
		return data['meteringPoints'][0]['values']