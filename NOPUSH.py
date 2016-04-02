import os
from pyiso import client_factory
from datetime import datetime, timedelta
import pytz

os.environ['ISONE_USERNAME'] = 'joelcofield@gmail.com'
os.environ['ISONE_PASSWORD'] = 'password'
os.environ['ENTSOe_USERNAME'] = 'joelcofield@gmail.com'
os.environ['ENTSOe_PASSWORD'] = 'password'

client = "ISONE"

isone = client_factory(client)
freqs = ['5m']
start_at = datetime(2014, 7, 25,0, 0, 0, 0,tzinfo=pytz.timezone('US/Eastern')).astimezone(pytz.utc)
end_at = start_at + timedelta(days=6)
print(start_at)
print(end_at)
for f in freqs:
    print("Getting %s load"%client)
    isone_load = isone.get_load(latest=False,start_at=start_at,end_at=end_at, frequency=f)
    print(len(isone_load))
    # print("Getting %s gen"%client)
    # isone_gen = isone.get_generation(latest=False,start_at=start_at,end_at=end_at, frequency=f)
    # print(len(isone_gen))
    # print("Getting ISONE lmp")
    # isone_lmp = isone.get_lmp(latest=True)
    # print(isone_lmp)

