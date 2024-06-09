import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt

# Turn timestamp into hours
def ts_to_hours(ts):
    ts = ts/100
    hours, rem = divmod(ts, 3600)
    minutes, secs = divmod(rem, 60)
    return "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(secs))



datafile = 'dataset9/data9.parquet'

# IP geolocation
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')

# Read parquet data file
data = pd.read_parquet(datafile)
#print(data.to_string())

print("Uploads: " + str(data['up_bytes'].describe()))
print("Downloads: " + str(data['down_bytes'].describe()))

#NET = ipaddress.IPv4Network('192.168.109.0/24')

#dns1 = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['dst_ip'])['up_bytes'].sum()
