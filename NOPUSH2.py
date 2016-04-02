import os
from pyiso import client_factory
from datetime import datetime, timedelta
import pytz
import pandas as pd
import pyiso

try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen  # Changed from urllib2 for python3.x
year_pieces = []
url = 'http://www.iso-ne.com/static-assets/documents/2015/05/2014_smd_hourly.xls'
local = '/users/jcofield/Downloads/2014_smd_hourly.xls'

url_bpa = 'http://transmission.bpa.gov/business/operations/wind/WindGenTotalLoadYTD_2014.xls'
# socket = urlopen(url)
cols = [0, 1, 14]
header_names = ['Hour', 'Load']
xd = pd.ExcelFile(local,parse_cols=cols,parse_dates=True, index_col=0,header_names=header_names)
# piece = pyiso.parse_to_df(xd, mode='xls', sheet_names=['ISONE CA'],
#                          skiprows=0, parse_cols=cols,
#                          parse_dates=True, index_col=0,
#                          header_names=header_names)

sheet_names = ["ISONE CA"]
pieces = []
for sheet in sheet_names:
    pieces.append(xd.parse(sheet))
df = pd.concat(pieces)
year_pieces.append(pieces)
print(year_pieces)
df = pd.concat(pieces)
print(df.head(3))
print("done")