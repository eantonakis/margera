import meteomatics.api as api
import datetime as dt
import mysql.connector


class ETL():
    def __init__(self, credentials, coords):
        self.credentials = credentials
        self.coords = coords
        startdate = dt.datetime.utcnow().replace(hour=1, minute=0, second=0, microsecond=0)
        enddate = startdate + dt.timedelta(days=7)
        interval = dt.timedelta(hours=1)
        parameters = ['t_2m:C', 'relative_humidity_2m:p', 'precip_3h:mm', 'wind_speed_2m:ms', 'wind_dir_2m:d']
        self.df = api.query_time_series(list(coords.values()), startdate, enddate, interval, parameters, self.credentials['meteomatics_username'], self.credentials['meteomatics_password'])

        self.transform_data()
        self.load_data()


    def transform_data(self):
        self.df.columns = ['temperature', 'relative_humidity', 'precipitation', 'wind_speed', 'wind_direction']

        self.df['datetime'] = self.df.index.get_level_values('validdate')
        self.df['date'] = self.df['datetime'].apply(lambda n: n.date)
        self.df['time'] = self.df['datetime'].apply(lambda n: n.time)
        self.df = self.df.drop('datetime', axis=1)

        self.df['latitude'] = self.df.index.get_level_values('lat')
        self.df['longtitude'] = self.df.index.get_level_values('lon')

        self.df = self.df.droplevel(['lat', 'lon'])
        self.df = self.df.reset_index()
        self.df = self.df.drop('validdate', axis=1)

        self.df['location'] = [list(self.coords.keys())[list(self.coords.values()).index((lat,lon))] for lat,lon in zip(self.df['latitude'], self.df['longtitude'])]


    def load_data(self):
        try:
            self.db = mysql.connector.connect(host="aws-weather-db.c4v43ogsh9cx.us-east-1.rds.amazonaws.com", user=self.credentials['db_username'], password=self.credentials['db_password'], database = "weatherdb")
        except:
            print("Database connection error!")
            exit(1)
        # try:
        #     self.db = mysql.connector.connect(host="localhost", user=self.credentials['db_username'], password=self.credentials['db_password'], database = "weatherdb")
        # except:
        #     print("Database connection error!")
        #     exit(1)

        self.cursor = self.db.cursor()
        # create 'weatherdb'
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS weatherdb")

        self.cursor.execute("DROP TABLE IF EXISTS Weather")

        #create table
        self.cursor.execute("""
                CREATE TABLE Weather(
                    Location VARCHAR(20), temperature FLOAT,
                    latitude FLOAT, longtitude FLOAT,
                    relative_humidity FLOAT, precipitation FLOAT,
                    wind_speed FLOAT, wind_direction FLOAT,
                    date DATE, time TIME, 
                    id INTEGER PRIMARY KEY AUTO_INCREMENT
                     )""")

        # populate table
        data = [
            (loc, temp, lat, lon, hum, prec, winds, windd, date, time) for loc, temp, lat, lon, hum, prec, winds, windd, date, time in 
            zip(
            self.df.loc[:,'location'], self.df.loc[:,'temperature'], self.df.loc[:,'latitude'], self.df.loc[:,'longtitude'],
            self.df.loc[:,'relative_humidity'], self.df.loc[:,'precipitation'], self.df.loc[:,'wind_speed'], self.df.loc[:,'wind_direction'],
            self.df.loc[:,'date'], self.df.loc[:,'time']
            )
            ]
        
        self.cursor.executemany("""INSERT INTO Weather (
            location, temperature, latitude, longtitude, relative_humidity, precipitation, wind_speed, wind_direction, date, time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)

        self.db.commit()


