import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 

datafile='./dataset9/test9.parquet'

### IP geolocalization
gi=pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2=pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')

data=pd.read_parquet(datafile)

NET=ipaddress.IPv4Network('192.168.109.0/24')
bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)

#Geolocalization of public destination adddress
cc=data[bpublic]['dst_ip'].apply(lambda y:gi.country_code_by_addr(y)).to_frame(name='cc')

dfbPublic = pd.DataFrame(bpublic,columns = ['pub'])
data2 = pd.concat([data, dfbPublic], axis=1)

addresses = data2.loc[(data2['pub']==False)].groupby(['dst_ip'])['proto'].count()
print(addresses)

# Output:
# 192.168.109.126      187
# 192.168.109.14       112
# 192.168.109.168      149
# 192.168.109.224    80856
# 192.168.109.225    70679
# 192.168.109.227    81417
# 192.168.109.230    72241
# 192.168.109.31       205
# 192.168.109.61       206


addresses = data2.loc[ (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
print(addresses.sort_values())

# Output:
# 192.168.109.38     4
# 192.168.109.25     4
# 192.168.109.26     4
# 192.168.109.27     4
# 192.168.109.28     4
#                   ..
# 192.168.109.61     7
# 192.168.109.14     8
# 192.168.109.31     8
# 192.168.109.126    8
# 192.168.109.168    8

addresses = data2.loc[((data2['src_ip']=='192.168.109.126') | (data2['src_ip']=='192.168.109.14') | (data2['src_ip']=='192.168.109.168') | (data2['src_ip']=='192.168.109.31') | (data2['src_ip']=='192.168.109.61')) & (data2['pub']==False) ].groupby(['src_ip'])['dst_ip'].unique()
print(addresses)

# ver melhor esta linha
addresses = data2.loc[((data2['src_ip']=='192.168.109.126') | (data2['src_ip']=='192.168.109.14') | (data2['src_ip']=='192.168.103.168') | (data2['src_ip']=='192.168.109.31') | (data2['src_ip']=='192.168.109.61')) & (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
print(addresses)

# Possiveis botnets
# Output:
# src_ip
# 192.168.109.126    8
# 192.168.109.14     8
# 192.168.109.31     8
# 192.168.109.61     7