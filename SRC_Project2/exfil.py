import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 

datafile='./dataset9/data9.parquet'
testfile = './dataset9/test9.parquet'

### IP geolocalization
gi=pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2=pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')

data=pd.read_parquet(datafile)
test=pd.read_parquet(testfile)

NET=ipaddress.IPv4Network('192.168.109.0/24')
bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)

#Geolocalization of public destination adddress
cc=data[bpublic]['dst_ip'].apply(lambda y:gi.country_code_by_addr(y)).to_frame(name='cc')


## version 2

# Check the number of bytes uploaded and downloaded
normal_up_down = data.loc[~data['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
# Check the number of flows to the internet
normal_flows = data.loc[~data['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).size().reset_index(name='counts')

# Check the number of bytes uploaded and downloaded - test
test_up_down = test.loc[~test['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
# Check the number of flows to the internet - test
test_flows = test.loc[~test['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).size().reset_index(name='counts')

# Average uploaded and downloaded bytes per flow
normal_up_down = pd.merge(normal_up_down, normal_flows, on='src_ip', how='inner')
normal_up_down['avg_up_bytes'] = normal_up_down['up_bytes'] / normal_up_down['counts']
normal_up_down['avg_down_bytes'] = normal_up_down['down_bytes'] / normal_up_down['counts']

# Average uploaded and downloaded bytes per flow - test
test_up_down = pd.merge(test_up_down, test_flows, on='src_ip', how='inner')
test_up_down['avg_up_bytes'] = test_up_down['up_bytes'] / test_up_down['counts']
test_up_down['avg_down_bytes'] = test_up_down['down_bytes'] / test_up_down['counts']

# Compare normal and test data
bytes_comparison = pd.merge(normal_up_down, test_up_down, on='src_ip', how='inner', suffixes=('_normal', '_test'))
exfiltration_candidates = bytes_comparison[
    (bytes_comparison['avg_up_bytes_test'] > (bytes_comparison['avg_up_bytes_normal'] * 1.5)) |
    (bytes_comparison['avg_down_bytes_test'] > (bytes_comparison['avg_down_bytes_normal'] * 1.5))
]


# Exfiltration bytes - test
exfilt_up_down = test.loc[(test['src_ip'].isin(exfiltration_candidates['src_ip'])) & (~test['dst_ip'].str.startswith('192.168.109.'))].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()

# Plot the results
exfilt_up_down.plot(x='src_ip', y=['up_bytes', 'down_bytes'], kind='bar', title="Exfiltration", xlabel="IP", ylabel="Bytes")
plt.savefig('./plots/exfiltration.png')
