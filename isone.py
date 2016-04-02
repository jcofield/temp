from pyiso.base import BaseClient
from pyiso import LOGGER
from os import environ
from datetime import datetime
import pytz
import pandas as pd


class ISONEClient(BaseClient):
    NAME = 'ISONE'

    base_url = 'https://webservices.iso-ne.com/api/v1.1'
    base_url_static = 'http://www.iso-ne.com/static-assets/documents'
    TZ_NAME = 'America/New_York'

    fuels = {
        'Coal': 'coal',
        'Hydro': 'hydro',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Oil': 'oil',
        'Solar': 'solar',
        'Wind': 'wind',
        'Wood': 'biomass',
        'Refuse': 'refuse',
        'Landfill Gas': 'biogas',
    }

    locations = {
        'MAINE': 4001,
        'NEWHAMPSHIRE': 4002,
        'VERMONT': 4003,
        'CONNECTICUT': 4004,
        'RHODEISLAND': 4005,
        'SEMASS': 4006,
        'WCMASS': 4007,
        'NEMASSBOST': 4008,
    }

    def __init__(self, *args, **kwargs):
        super(ISONEClient, self).__init__(*args, **kwargs)
        try:
            self.auth = (environ['ISONE_USERNAME'], environ['ISONE_PASSWORD'])
        except KeyError:
            msg = 'Must define environment variables ISONE_USERNAME and ISONE_PASSWORD to use ISONE client.'
            raise RuntimeError(msg)

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for endpoint in self.request_endpoints():
            # carry out request
            data = self.fetch_data(endpoint, self.auth)
            raw_data += data['GenFuelMixes']['GenFuelMix']

        # parse data
        for raw_dp in raw_data:
            # set up storage
            parsed_dp = {}

            # add values
            parsed_dp['timestamp'] = self.utcify(raw_dp['BeginDate'])
            parsed_dp['gen_MW'] = raw_dp['GenMw']
            parsed_dp['fuel_name'] = self.fuels[raw_dp['FuelCategory']]
            parsed_dp['ba_name'] = self.NAME
            parsed_dp['market'] = self.options['market']
            parsed_dp['freq'] = self.options['frequency']

            # add to full storage
            parsed_data.append(parsed_dp)

        return parsed_data

    def get_load(self, latest=False, start_at=False, end_at=False,
                 forecast=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, forecast=forecast,
                            start_at=start_at, end_at=end_at, **kwargs)

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data: web api
        for endpoint in self.request_endpoints():
            # carry out request
            data = self.fetch_data(endpoint, self.auth)

            # pull out data
            try:
                raw_data += self.parse_json_load_data(data)
            except ValueError as e:
                LOGGER.warn(e)
                continue

        # parse data
        now = pytz.utc.localize(datetime.utcnow())
        for raw_dp in raw_data:
            # set up storage
            parsed_dp = {}

            # add values
            parsed_dp['timestamp'] = self.utcify(raw_dp['BeginDate'])
            parsed_dp['load_MW'] = raw_dp['LoadMw']
            parsed_dp['ba_name'] = self.NAME
            parsed_dp['market'] = self.options['market']
            parsed_dp['freq'] = self.options['frequency']

            # add to full storage
            if self.options['forecast'] and parsed_dp['timestamp'] < now:
                # don't include past forecast data
                pass
            else:
                parsed_data.append(parsed_dp)

        # collect raw data: static historical documents
        api_begin_date = self.utcify(datetime(2014,7,31))

        # set up requests
        request_urls = []
        if self.options['start_at'] < api_begin_date:
            this_year = self.options['start_at'].year
            while this_year <= api_begin_date.year: #TODO: consider if starts after August 2014
                if 2003 <= this_year <= 2013:
                    request_urls.append(self.base_url_static +
                                        '/markets/hstdata/znl_info/hourly/%s_smd_hourly.xls' % (this_year))
                elif this_year == 2014: # Hardcoded until urls are documented
                    request_urls.append(self.base_url_static + '/2015/05/2014_smd_hourly.xls')
                elif this_year >= 2015:
                    request_urls.append(self.base_url_static + '/%s/02/smd_hourly.xls' % (this_year))
                else:
                    raise ValueError('Cannot get ISONE load data before 2003.')
                this_year += 1

        # carry out request
        if request_urls:
            data = self.fetch_static(request_urls)

        return parsed_data
    def fetch_static(self, request_urls):

        # set up columns to get
        mode = self.options['data']
        if mode == 'load':
            cols = [0, 1, 14]
            header_names = ['Hour', 'Load']
        elif mode == 'lmp':
            cols = [0, 1, 8]
            header_names = ['Hour', 'LMP']

        # get each year of data
        pieces = []
        for url in request_urls:
            xd = self.fetch_xls(url)
            piece = self.parse_to_df(xd, mode='xls', sheet_names=['ISONE CA'],
                                 skiprows=0, parse_cols=cols,
                                 parse_dates=True, index_col=0,
                                 header_names=header_names)
            pieces.append(piece)
        return pd.concat(pieces)
    def handle_options(self, **kwargs):
        # default options
        super(ISONEClient, self).handle_options(**kwargs)

        # handle market
        if not self.options.get('market'):
            if self.options['data'] == 'gen':
                # generation on n/a market
                self.options['market'] = self.MARKET_CHOICES.na
            else:
                # load on real-time 5-min or hourly forecast
                if self.options['forecast']:
                    self.options['market'] = self.MARKET_CHOICES.dam
                else:
                    self.options['market'] = self.MARKET_CHOICES.fivemin

        # handle frequency
        if not self.options.get('frequency'):
            if self.options['data'] == 'gen':
                # generation on n/a frequency
                self.options['frequency'] = self.FREQUENCY_CHOICES.na
            else:
                # load on real-time 5-min or hourly forecast
                if self.options['market'] == self.MARKET_CHOICES.dam:
                    self.options['frequency'] = self.FREQUENCY_CHOICES.dam
                else:
                    self.options['frequency'] = self.FREQUENCY_CHOICES.fivemin

    def request_endpoints(self, location_id=None):
        """Returns a list of endpoints to query, based on handled options"""
        # base endpoint
        ext = ''
        if self.options['data'] == 'gen':
            base_endpoint = 'genfuelmix'
        elif self.options['data'] == 'lmp' and location_id is not None:
            base_endpoint = 'fiveminutelmp'
            ext = '/location/%s' % location_id
        elif self.options['market'] == self.MARKET_CHOICES.dam:
            base_endpoint = 'hourlyloadforecast'
        else:
            base_endpoint = 'fiveminutesystemload'

        # set up storage
        request_endpoints = []

        # handle dates
        if self.options['latest']:
            request_endpoints.append('/%s/current%s.json' % (base_endpoint, ext))

        elif self.options['start_at'] and self.options['end_at']:
            for date in self.dates():
                date_str = date.strftime('%Y%m%d')
                request_endpoints.append('/%s/day/%s%s.json' % (base_endpoint, date_str, ext))

        else:
            msg = 'Either latest or forecast must be True, or start_at and end_at must both be provided.'
            raise ValueError(msg)

        # return
        return request_endpoints

    def fetch_data(self, endpoint, auth):
        url = self.base_url + endpoint
        response = self.request(url, auth=auth)
        if response:
            return response.json()
        else:
            return {}

    def fetch_historical(self):
        """Get ISONE lmp or load data from the far past"""
        # set up requests
        request_urls = []
        this_year = self.options['start_at'].year
        while this_year <= self.options['start_at'].year:
            if this_year >= 2003 and this_year <= 2013:
                request_urls.append(self.base_url_static +
                                    'markets/hstdata/znl_info/hourly/%s_smd_hourly.x' % (this_year))
            elif this_year <= 2016:
                request_urls.append(self.base_url_static +
                    'markets/hstdata/znl_info/hourly/%s_smd_hourly.x' % (this_year))
            else:
                raise ValueError('Cannot get ISONE generation data before 2003.')
            this_year += 1

        # set up columns to get
        mode = self.options['data']
        if mode == 'gen':
            cols = [0, 2, 4, 5]
            header_names = ['Wind', 'Hydro', 'Thermal']
        elif mode == 'load':
            cols = [0, 3]
            header_names = ['Load']
        else:
            raise ValueError('Cannot fetch data without a data mode')

        # get each year of data
        pieces = []
        for url in request_urls:
            xd = self.fetch_xls(url)
            piece = self.parse_to_df(xd, mode='xls', sheet_names=xd.sheet_names,
                                     skiprows=18, parse_cols=cols,
                                     parse_dates=True, index_col=0,
                                     header_names=header_names)
            pieces.append(piece)

        # return
        df = pd.concat(pieces)
        return df

    def parse_json_load_data(self, data):
        """
        Pull approriate keys from json data set.
        Raise ValueError if parser fails.
        """
        try:
            if self.options.get('latest'):
                return data['FiveMinSystemLoad']
            elif self.options['market'] == self.MARKET_CHOICES.dam:
                return data['HourlyLoadForecasts']['HourlyLoadForecast']
            else:
                return data['FiveMinSystemLoads']['FiveMinSystemLoad']
        except (KeyError, TypeError):
            raise ValueError('Could not parse ISONE load data %s' % data)

    def parse_json_lmp_data(self, data):
        """
        Pull approriate keys from json data set.
        Raise ValueError if parser fails.
        """
        try:
            if self.options.get('latest'):
                return data['FiveMinLmp']
            else:
                return data['FiveMinLmps']['FiveMinLmp']
        except (KeyError, TypeError):
            raise ValueError('Could not parse ISONE lmp data %s' % data)

    def get_lmp(self, node_id, latest=True, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='lmp', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get location id
        try:
            locationid = self.locations[node_id.upper()]
        except KeyError:
            raise ValueError('No LMP data available for location %s' % node_id)

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for endpoint in self.request_endpoints(locationid):
            # carry out request
            data = self.fetch_data(endpoint, self.auth)

            # pull out data
            try:
                raw_data += self.parse_json_lmp_data(data)
            except ValueError as e:
                LOGGER.warn(e)
                continue

        # parse data
        for raw_dp in raw_data:
            # set up storage
            parsed_dp = {}

            # add values
            parsed_dp['timestamp'] = self.utcify(raw_dp['BeginDate'])
            parsed_dp['lmp'] = raw_dp['LmpTotal']
            parsed_dp['ba_name'] = self.NAME
            parsed_dp['market'] = self.options['market']
            parsed_dp['freq'] = self.options['frequency']
            parsed_dp['node_id'] = node_id
            parsed_dp['lmp_type'] = 'energy'

            # add to full storage
            to_store = True
            if self.options['sliceable']:
                if self.options['start_at'] > parsed_dp['timestamp'] or self.options['end_at'] < parsed_dp['timestamp']:
                    to_store = False
            if to_store:
                parsed_data.append(parsed_dp)

        return parsed_data
