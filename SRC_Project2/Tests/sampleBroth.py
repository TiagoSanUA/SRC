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

datafile='./dataset9/test9.parquet'
# datafile='./dataset9/data9.parquet'
data=pd.read_parquet(datafile)
# Initialize GeoIP databases
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')

### IP geolocalization
# Check if the IP address is in the dataset
addr='193.136.713.58'

ip_present = addr in data['src_ip'].values or addr in data['dst_ip'].values

if ip_present:
    # Perform geolocation lookup
    cc = gi.country_code_by_addr(addr)
    org = gi2.org_by_addr(addr)

    print("cc: ", cc, "org: ", org)
else:
    print(f"The IP address {addr} is not present in the dataset.")

### DNS resolution
addr=dns.resolver.resolve("www.ua.pt", 'A')
print("\nDNS resolution part:")
for a in addr:
    print(a)
    
### Reverse DNS resolution  
print("\nReverse DNS part: ")  
name=dns.reversename.from_address("193.136.173.58")
addr=dns.resolver.resolve(name, 'PTR')
for a in addr:
    print(a)

### Read parquet data files
# data=pd.read_parquet(datafile)
#print(data.to_string())

#Just the UDP flows
udpF=data.loc[data['proto']=='udp']

#Number of UDP flows for each source IP
nudpF=data.loc[data['proto']=='udp'].groupby(['src_ip'])['up_bytes'].count()

# nudpF=data.loc[data['proto']=='udp'].groupby(['src_ip'])['up_bytes'].sum().sort_values()
# plt.show()

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
NET=ipaddress.IPv4Network('192.168.109.0/24')
bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)
dfbPublic = pd.DataFrame(bpublic,columns = ['pub'])
data2 = pd.concat([data, dfbPublic], axis=1)
addresses = data2.loc[(data2['pub']==False)].groupby(['dst_ip'])['port'].count()
print("\naddresses: ",addresses)
# print("\n\nganza\n")


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

print("\nudp na porta 53:")
dns1 = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['dst_ip'])['up_bytes'].sum().sort_values()
print(dns1)

###########################################################################################################
# ver numero de conexoes para os ips DNS
# testei com 109.227 em vez de 230- rodrigo
conns = data.loc[(data['dst_ip']=='192.168.109.225') | (data['dst_ip']=='192.168.109.230')].groupby(['src_ip'])['up_bytes'].count()
# traffic_down = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])['down_bytes'].sum()
# ratio = traffic_up/traffic_down
print('\nips que se ligam aos DNS\'s')
print(conns.sort_values()[-20:].to_string())


############################################## testing ################################################################################# updated Rodrigo
traffic_up = data.loc[(data['dst_ip']=='192.168.109.225') | (data['dst_ip']=='192.168.109.230')].groupby(['src_ip'])['up_bytes'].sum()
traffic_down = data.loc[(data['dst_ip']=='192.168.109.225') | (data['dst_ip']=='192.168.109.230')].groupby(['src_ip'])['down_bytes'].sum()
ratio = traffic_up/traffic_down
print("\nUp and Down ratio for the Dns Servers:")
print(ratio.sort_values())

dns_traffic = data.loc[(data['dst_ip']=='192.168.109.225') | (data['dst_ip']=='192.168.109.230')].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')
print('\nsum of connections to the dns servers')
print(dns_traffic)


print("\nup and down bytes where udp 443")
traffic_up = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['up_bytes'].sum()
traffic_down = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values())

print("\ndns with certain src ip's")
dns_conns_up = data.loc[((data['dst_ip']=='192.168.109.225') | (data['dst_ip']=='192.168.109.230')) & ((data['src_ip']=='192.168.109.59') | (data['src_ip']=='192.168.109.200'))].groupby(['src_ip'])['up_bytes'].sum()
dns_conns_down = data.loc[((data['dst_ip']=='192.168.109.236') | (data['dst_ip']=='192.168.109.230')) & ((data['src_ip']=='192.168.109.59') | (data['src_ip']=='192.168.109.200'))].groupby(['src_ip'])['down_bytes'].sum()
ratio =(dns_conns_up/dns_conns_down).sort_values()
print(ratio)

avgUp=data.groupby(['src_ip'])['down_bytes'].mean()
print(" average down_bytes for each src_ip and prints the sorted results")
print(avgUp.sort_values())


base = data.loc[((data['proto']=='tcp'))].groupby(['src_ip', 'dst_ip'])
traffic_up = base['up_bytes'].sum()
traffic_down = base['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values())

conns = data2.loc[(data2['pub']==False)].groupby(['src_ip', 'dst_ip'])['port'].count()
print(conns.sort_values()[-30:])

conns = data.groupby(['src_ip', 'dst_ip'])['port'].count()
print(conns.sort_values()[-30:])

avgUp=data.groupby(['src_ip'])['up_bytes'].mean()
print(avgUp.sort_values())

traffic = data[['src_ip', 'up_bytes', 'down_bytes']].groupby(['src_ip']).sum().sort_values(by='down_bytes')
print(traffic)


data['timestamp'] = (data['timestamp'] / 100).astype(int)

base = data.groupby(['src_ip', 'dst_ip'])
up = base['up_bytes'].mean()
down = base['down_bytes'].mean()
print('\n')
print((up).sort_values()[-50:])
#135kb
