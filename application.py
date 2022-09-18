from endpoints import Endpoints
from flask import Flask

CREDENTIALS = {
    'meteomatics_username': 'margera_antonakis',
    'meteomatics_password': 'C5vyTu9j5M',
    'db_username': 'admin',
    'db_password': 'margera1234'
    }

# CREDENTIALS = {
#     'meteomatics_username': 'margera_antonakis',
#     'meteomatics_password': 'C5vyTu9j5M',
#     'db_username': 'root',
#     'db_password': 'stathis1234'
#     }

COORDS = {
    'New York' : (40.71, -74.00),
    'Berlin' : (52.51, 13.38),
    'Sydney': (-33.85, 151.21)
    }


endpoints = Endpoints(CREDENTIALS, COORDS)
application = Flask(__name__)


@application.route('/')
def index():
    return 'Stathis Antonakis'


@application.route('/weather/locations/')
def get_locations():
    return endpoints.get_locations()


@application.route('/weather/last_forecasts_per_day')
def get_last_forecasts_per_day():
    return endpoints.get_last_forecasts_per_day()


@application.route('/weather/get_last_forcasts_avg_temp/<n>')
def get_last_forcasts_avg_temp(n='3'):
    return endpoints.get_last_forcasts_avg_temp(n)


@application.route('/weather/get_top_locations_per_/<metric>/<n>')
def get_top_locations_per_metric(metric='temperature', n='1'):
    return endpoints.get_top_locations_per_metric(metric, n)



if __name__ == "__main__": 
    application.run()
