from ETL import ETL

class Endpoints():
    def __init__(self, credentials, coords):
        self.coords = coords
        self._etl = ETL(credentials, self.coords)
        self.cursor = self._etl.cursor


    def get_locations(self):
        self.cursor.execute("""
        SELECT DISTINCT location FROM Weather
         """)

        return {'Locations': [n for n in self.cursor]}


    def get_last_forecasts_per_day(self):
        self.cursor.execute("""
        SELECT * FROM Weather
        WHERE time = (SELECT MAX(time) FROM Weather)
        GROUP BY date, location
         """)

        labels = ['Location', 'Temperature', 'Latitude', 'Longtitude', 'Relative Humidity', 'Precipitation', 'Windspeed', 'Wind Direction', 'Date', 'Time', 'id']
        return self.response(labels)


    def get_last_forcasts_avg_temp(self, n):
        n=int(n)
        self.cursor.execute("""
        SELECT location, date, AVG(temperature) FROM Weather
        JOIN (SELECT DISTINCT time FROM Weather ORDER BY time DESC LIMIT %d) a
        ON a.time = Weather.time
        GROUP BY date, location
        """%(n))

        labels = ['Location', 'date', 'avg_temperature']
        return self.response(labels)

    
    def get_top_locations_per_metric(self, metric, n):
        n=int(n)
        if metric not in ['Location', 'temperature', 'latitude', 'longtitude', 'relative_humidity', 'precipitation', 'wind_speed', 'wind_direction', 'date', 'time', 'id']:
            return f"Invalid requested metric: '{metric}'."

        self.cursor.execute("""
        SELECT location, MAX(%s) max_temperature FROM Weather
        GROUP BY location
        ORDER BY max_temperature DESC
        LIMIT %d
        """%(metric, n))

        labels = ['Location', f'max_{metric}']
        return self.response(labels)


    def response(self, labels):
        response = {}
        for lbl in labels:
            response[lbl] = []
        for row in self.cursor:
            for label, col in zip(labels, row):
                response[label].append(str(col))
        return response



        

