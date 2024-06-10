# pip install pygeoip
# pip install fastparquet 
# pip install dnspython
import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt 

#datafile='./dataset9/test9.parquet'
datafile='./dataset9/data9.parquet'

### IP geolocalization
gi=pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2=pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')
addr='193.136.73.21'
cc=gi.country_code_by_addr(addr)
org=gi2.org_by_addr(addr)
print(cc,org)

### DNS resolution
addr=dns.resolver.resolve("www.ua.pt", 'A')
for a in addr:
    print(a)
    
### Reverse DNS resolution    
name=dns.reversename.from_address("193.136.172.20")
addr=dns.resolver.resolve(name, 'PTR')
for a in addr:
    print(a)

### Read parquet data files
data=pd.read_parquet(datafile)
#print(data.to_string())

#Just the UDP flows
udpF=data.loc[data['proto']=='udp']

#Number of UDP flows for each source IP
nudpF=data.loc[data['proto']=='udp'].groupby(['src_ip'])['up_bytes'].count()

#nudpF=data.loc[data['proto']=='udp'].groupby(['src_ip'])['up_bytes'].sum().sort_values()
#plt.show()

#Number of UDP flows to port 443, for each source IP
nudpF443=data.loc[(data['proto']=='udp')&(data['port']==443)].groupby(['src_ip'])['up_bytes'].count()

#Average number of downloaded bytes, per flow, for each source IP
avgUp=data.groupby(['src_ip'])['down_bytes'].mean()


#Total uploaded bytes to destination port 443, for each source IP, ordered from larger amount to lowest amount
upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False)

#Histogram of the total uploaded bytes to destination port 443, by source IP
upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().hist()
#plt.show()

#Is destination IPv4 a public address?
NET=ipaddress.IPv4Network('192.168.103.0/24')
#bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)
#dfbPublic = pd.DataFrame(bpublic,columns = ['pub'])
#data2 = pd.concat([data, dfbPublic], axis=1)
#addresses = data2.loc[(data2['pub']==False)].groupby(['dst_ip'])['port'].count()
#print(addresses)
print("\n\nganza\n")


""" 
port  proto
53    udp      116966
443   tcp      865580
      udp        2409 
"""


traffic_up = data.groupby(['src_ip'])['up_bytes'].sum()
traffic_down = data.groupby(['src_ip'])['down_bytes'].sum()

#print(traffic)
	#ratio de trafego por IP
#print((traffic_up/traffic_down).sort_values().to_string())


dns1 = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['dst_ip'])['up_bytes'].sum().sort_values()
print(dns1)

###########################################################################################################
# ver numero de conexoes para os ips DNS
conns = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])['up_bytes'].count()
# traffic_down = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
print(conns.sort_values()[-20:].to_string())


############################################## testing #################################################################################
# traffic_up = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

# dns_traffic = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')


# print(dns_traffic)


# google = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['up_bytes'].sum().sort_values()
# print(google)


# traffic_up = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())


# traffic = data.loc[(data['proto']=='udp') & (data['port']==443) & ((data['src_ip']=='192.168.103.43') | (data['src_ip']=='192.168.103.47'))].groupby(['src_ip','dst_ip']).agg({'dst_ip': 'count', 'up_bytes': 'sum'})
# print(traffic.sort_values(by='up_bytes'))

# traffic_up = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

#traffic_up = data.loc[(data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')].groupby(['src_ip'])['up_bytes'].count().sort_values()
# traffic_down = data.loc[(data['dst_ip']=='192.168.103.236')].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
#print(traffic_up)

# dns_conns_up = data.loc[((data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['up_bytes'].sum()
# dns_conns_down = data.loc[((data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['down_bytes'].sum()
# ratio =(dns_conns_up/dns_conns_down).sort_values()
# print(ratio)

# dns_conns_up = data.loc[((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['up_bytes'].sum()
# dns_conns_down = data.loc[ ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['down_bytes'].sum()
# ratio =(dns_conns_up/dns_conns_down).sort_values()
# print(ratio)

#Geolocalization of public destination adddress
#cc=data[bpublic]['dst_ip'].apply(lambda y:gi.country_code_by_addr(y)).to_frame(name='cc')

#servidores DNS: 192.168.103.235, 192.168.103.236

# avgUp=data.groupby(['src_ip'])['down_bytes'].mean()
# print(avgUp.sort_values())



# traffic = data.loc[(data['src_ip']=='192.168.103.110') | (data['src_ip']=='192.168.103.207')].groupby(['src_ip','dst_ip']).agg({'dst_ip': 'count', 'up_bytes': 'sum'}).rename(columns={'dst_ip': 'connections'})
# print(traffic.sort_values(by='connections')[-15:])


# addresses = data2.loc[ (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
# print(addresses.sort_values())



#print(data.loc[data['src_ip'].isin(['192.168.103.47', '192.168.103.43'])].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum().sort_values()[-15:])


# base = data.loc[((data['proto']=='tcp'))].groupby(['src_ip', 'dst_ip'])
# traffic_up = base['up_bytes'].sum()
# traffic_down = base['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

# conns = data2.loc[(data2['pub']==False)].groupby(['src_ip', 'dst_ip'])['port'].count()
# print(conns.sort_values()[-30:])

# conns = data.groupby(['src_ip', 'dst_ip'])['port'].count()
# print(conns.sort_values()[-30:])

# base = data.loc[data['src_ip'].isin(['192.168.103.110','192.168.103.207'])].groupby(['src_ip', 'dst_ip'])
# traffic_up = base['up_bytes'].sum()
# traffic_down = base['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values()[-15:])

# avgUp=data.groupby(['src_ip'])['up_bytes'].mean()
# print(avgUp.sort_values())

# traffic = data[['src_ip', 'up_bytes', 'down_bytes']].groupby(['src_ip']).sum().sort_values(by='down_bytes')
# print(traffic)

# dns_traffic = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='down_bytes')
# print(dns_traffic[-20:])


# base = data.loc[(data['src_ip']=='192.168.103.121') & data['dst_ip'].isin(['192.168.103.235', '192.168.103.236'])].groupby(['src_ip'])
# up = base['up_bytes'].sum()
# down = base['down_bytes'].sum()
# ratio = up/down
# print(ratio.sort_values())
      
# base = data.loc[(data['dst_ip'].isin(['192.168.103.222', '192.168.103.238','192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])
# traffic_up = base['up_bytes'].sum()
# traffic_down = base['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())


# base = data.loc[(data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])
# conns = base['dst_ip'].count()
# print(conns.sort_values())

# bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)
# dfbPublic = pd.DataFrame(bpublic,columns = ['bpublic'])
# data2 = pd.concat([data, dfbPublic], axis=1)

# traffic_up = data2.loc[(data2['src_ip'].isin(['192.168.103.78', '192.168.103.107'])) & (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data2['src_ip'].isin(['192.168.103.78', '192.168.103.107'])) & (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

# ######################################################################################################### 
# dados = data2.loc[(data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])[['up_bytes', 'down_bytes']].sum()
# print(dados.sort_values('down_bytes'))


# base = data.loc[(data['dst_ip'].isin(['192.168.103.222', '192.168.103.238', '192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].mean()
# print(base.sort_values(by='down_bytes'))

# traffic_up = data2.loc[ (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
# traffic_down = data2.loc[(data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())


# traffic_up = data.loc[ (data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data['dst_ip'].isin([ '192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

# traffic_up = data.loc[ (data['dst_ip'].isin([ '192.168.103.222', '192.168.103.238']))].groupby(['src_ip'])['up_bytes'].sum()
# traffic_down = data.loc[(data['dst_ip'].isin(['192.168.103.222', '192.168.103.238']))].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values())

# dns_traffic = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')
# print(dns_traffic[-20:]) 

# base = data.loc[(data['dst_ip'].isin([ '192.168.103.238', '192.168.103.222']))].groupby(['src_ip'])
# conns = base['dst_ip'].count()
# print(conns.sort_values())

# hist_data = data.groupby(['timestamp'])['up_bytes'].sum()

# print(hist_data[-600:].to_string())

# base = data.loc[(data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum()
# print(base.sort_values('up_bytes'))


# base = data.loc[(data['src_ip'].isin(['192.168.103.235', '192.168.103.236']))]
# print(base.sort_values('up_bytes'))


# base = data.loc[(data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].mean()
# print(base.sort_values('down_bytes')[-50:])

# base = data.groupby([ 'dst_ip'])[['up_bytes']].mean()
# print(base.sort_values(by='up_bytes')[-30:])



# print(data)
# print(data.sort_values(by="index"))

data['timestamp'] = (data['timestamp'] / 100).astype(int)
"""data['time'] = pd.to_datetime(data['timestamp'], unit='s').dt.strftime('%H:%M') """

##########################################################################################################
# get ammount of up/down  per minute
""" base = data.groupby(['time'])[['up_bytes']].sum()
print(base.sort_values('time').sort_values(by='up_bytes')[-20:]) """

""" base = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])
up = base['up_bytes'].mean()
down = base['down_bytes'].mean()
ratio = up/down
print(up.sort_values()) """

# base = data.loc[data['src_ip'].isin(['192.168.103.110','192.168.103.207'])].groupby(['src_ip', 'dst_ip'])
# traffic_up = base['up_bytes'].sum()
# traffic_down = base['down_bytes'].sum()
# ratio = traffic_up/traffic_down
# print(ratio.sort_values()[-15:])

""" result = data.groupby('dst_ip').agg({'up_bytes': 'sum', 'src_ip': lambda x: list(set(x))})
result = result.sort_values('up_bytes', ascending=True)
last_two_values = result.tail(2)
print("\n")
print(last_two_values) """


base = data.groupby(['src_ip', 'dst_ip'])
up = base['up_bytes'].mean()
down = base['down_bytes'].mean()
print((up).sort_values()[-50:])
#135kb
