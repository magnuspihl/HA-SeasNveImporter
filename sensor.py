import datetime, requests, mysql.connector, logging
from homeassistant.helpers.entity import Entity
_LOGGER = logging.getLogger(__name__)

CONF_DB_HOST = "database_host"
CONF_DB_USERNAME = "database_username"
CONF_DB_PASSWORD = "database_password"
CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_NAME = "name"
CONF_START_DATE = "start_date"
DEFAULT_NAME = "seasnve_energy"
ICON = "mdi:counter"

ATTR_DEVICE_CLASS = "device_class"
ATTR_STATE_CLASS = "state_class"
ATTR_LAST_RESET = "last_reset"

def log(message):
    _LOGGER.warning(message)
    print(message)

"""
PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
	{
		vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
		vol.Required(CONF_USERNAME): cv.string,
		vol.Required(CONF_PASSWORD): cv.string,
		vol.Required(CONF_DB_HOST): cv.string,
		vol.Required(CONF_DB_USERNAME): cv.string,
		vol.Required(CONF_DB_PASSWORD): cv.string,
        vol.Required(CONF_START_DATE): cv.string
	}
)
"""
def setup_platform(hass, config, add_entities, discovery_info=None):
    """Set up the SEAS-NVE sensor."""
    add_entities([SeasNveEnergyImporter(config[CONF_DB_HOST], config[CONF_DB_USERNAME], config[CONF_DB_PASSWORD], config[CONF_USERNAME], config[CONF_PASSWORD], config[CONF_NAME], config[CONF_START_DATE])], True)

class SeasNveEnergyImporter(Entity):
    def __init__(self, dbHost, dbUsername, dbPassword, username, password, name, startDate):
        self._dbHost = dbHost
        self._dbUsername = dbUsername
        self._dbPassword = dbPassword
        self._username = username
        self._password = password
        self._name = name
        self._startDate = startDate
        self._info = self._state = None

    @property
    def name(self):
        """Returns the name of the sensor."""
        return self._name
	
    @property
    def state(self):
        """Returns the state of the sensor."""
        return 0
	
    @property
    def device_state_attributes(self):
        """Returns the state attributes."""
        return {
            ATTR_DEVICE_CLASS: self.device_class,
            ATTR_STATE_CLASS: self.state_class,
            ATTR_LAST_RESET: self.last_reset
        }
	
    @property
    def unit_of_measurement(self):
        """Return the unit this state is expressed in."""
        return "kWh"
	
    @property
    def icon(self):
        """Icon to use in the frontend, if any."""
        return ICON
    
    @property
    def device_class(self):
        return "energy"

    @property
    def last_reset(self):
        return datetime.datetime.today().isoformat()

    @property
    def state_class(self):
        return "measurement"
	
    def update(self):
        """Get the latest data from the API and update the states."""
        data = self.fetchNewData()
        self.writeDataToLog(data)
        return

    def fetchNewData(self):
        stats = StatisticsDb()
        stats.connect(self._dbHost, self._dbUsername, self._dbPassword)
        mId = stats.getMetadataId(f"sensor.{self._name}")
        if (mId is None):
            return []
        lastWrite = stats.getLastStatistic(mId)
        if (lastWrite is None or lastWrite['sum'] == 0):
            lastWrite = { 'created': datetime.datetime.strptime(self._startDate, '%Y-%m-%d'), 'sum': 0 }
        api = SeasNveApi(self._username, self._password)
        now = datetime.datetime.now()
        start = lastWrite['created']
        data = api.consumption('Hour', start.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
        sum = lastWrite['sum']
        result = []
        for val in data:
            if val['value'] == 0.0:
                continue
            vStart = datetime.datetime.strptime(val['start'], '%Y-%m-%dT%H:%M:%S.%fz')
            if (vStart <= start):
                continue
            vEnd = datetime.datetime.strptime(val['end'], '%Y-%m-%dT%H:%M:%S.%fz')
            sum += val['value']
            result.append({'metadataId': mId, 'startTime': vStart, 'endTime': vEnd, 'value': val['value'], 'sum': {sum}})
        stats.close()
        return result

    def writeDataToStatistics(self, data):
        stats = StatisticsDb()
        stats.connect(self._dbHost, self._dbUsername, self._dbPassword)
        stats.cleanStatistics(data[0]['metadataId'])
        for d in data:
            stats.writeStatistic(d['metadataId'], d['startTime'], d['endTime'], d['value'], d['sum'])
        stats.close()
    
    def writeDataToLog(self, data):
        for d in data:
            log(f"SeasNveEnergy sensor wrote data: (metadataId) {d['metadataId']}, (startTime) {d['startTime']}, (endTime) {d['endTime']}, (value) {d['value']}, (sum) {d['sum']}")

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
			log(e)

	def getMeteringPoint(self, type):
		m = requests.get(base_url+'/profile/metering/', headers=authheader)
		data = m.json()[0]	# TODO: Search meteringpoints for meterType == type
		meteringPoint = data['meteringPoint']
		return meteringPoint

	def consumption(self,aggr,start,end):
		# Valid aggr is Hour, Day, Month, Year
		mpn = self.getMeteringPoint('Power')
		m = requests.get(f'{base_url}/profile/consumption/?meteringpoints={mpn}&start={start}&end={end}&aggr={aggr}', headers=authheader)
		data = m.json()
		return data['meteringPoints'][0]['values']

class StatisticsDb:

    def connect(self, dbHost, dbUser, dbPassword):
        # homeassistant:4DB1NIaIh24l@core-mariadb/homeassistant?charset=utf8
        self.connection = mysql.connector.connect(
            host=dbHost,
            user=dbUser,
            password=dbPassword,
            database="homeassistant")
        self.db = self.connection.cursor()
    
    def close(self):
        self.connection.close()

    def getMetadataId(self, sensorId):
        self.db.execute(f"SELECT id FROM statistics_meta WHERE statistic_id = '{sensorId}' LIMIT 1")
        for v in self.db:
            return v[0]
    
    def getLastStatistic(self, metadataId):
        self.db.execute(f"SELECT created,sum from statistics WHERE metadata_id={metadataId} ORDER BY created DESC LIMIT 1")
        for v in self.db:
            return {'created':v[0], 'sum':v[1]}

    def writeStatistic(self, metadataId, startTime, endTime, value, sum):
        self.db.execute(f"INSERT INTO statistics (`id`, `created`, `metadata_id`, `start`, `mean`, `min`, `max`, `last_reset`, `state`, `sum`) VALUES (NULL, '{endTime}', {metadataId}, '{startTime}', NULL, NULL, NULL, '{endTime}', {value}, {sum});")
        self.connection.commit()
    
    def cleanStatistics(self, metadataId):
        self.db.execute(f"DELETE FROM statistics WHERE metadata_id={metadataId} AND state=0")
        self.connection.commit()


importer = SeasNveEnergyImporter('192.168.1.201', 'homeassistant', '4DB1NIaIh24l', 'magnus.pihl@gmail.com', 'PqmMW2mu6X1w', 'seasnve_energy', '2016-04-01')
importer.update()