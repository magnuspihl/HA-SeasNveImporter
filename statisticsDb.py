import mariadb

class StatisticsDb:

    def connect(self, dbHost, dbUser, dbPassword):
        # homeassistant:4DB1NIaIh24l@core-mariadb/homeassistant?charset=utf8
        self.connection = mariadb.connect(
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
        self.db.execute(f"DELETE FROM statistics WHERE metadata_id={metadataId}")
        self.connection.commit()

# stats = StatisticsDb()
# stats.connect()
# mId = stats.getMetadataId('sensor.energy_this_month')
# print(f"Found sensor with metadata id {mId}")
# lastWrite = stats.getLastStatistic(mId)
# print(f"Last write for sensor {mId} was made at {lastWrite}")
# #stats.writeStatistic(mId, '2021-11-23 22:30:10', '2021-11-23 23:30:10', 50, 6629.194995117185)
# stats.close()
# print("Done")