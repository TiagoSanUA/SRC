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



datafile = 'dataset9/test9.parquet'

#############################################################################################
#       Getting the data from the parquet file
#############################################################################################

# IP geolocation
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')



# Example IP address for geolocation
addr = '193.136.73.21'
cc = gi.country_code_by_addr(addr)
org = gi2.org_by_addr(addr)
#print(cc, org)

# Read parquet data file
data = pd.read_parquet(datafile)
#print(data.to_string())

#put all Timestamps in hours
data['ts_to_hours'] = data['timestamp'].apply(lambda x: ts_to_hours(x))
# print(data['ts_to_hours'])

external_flows = data.loc[(data['src_ip'].str.startswith('192.168.109.')) & (~data['dst_ip'].str.startswith('192.168.109.'))]

# Number of UDP flows for each source IP (external flows)
nudpF_external = external_flows.loc[external_flows['proto']=='udp'].groupby(['src_ip']).count()
print("Number of UDP flows for each source IP: " + str(nudpF_external))

