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
print(data['ts_to_hours'])

print("Uploads: " + str(data['up_bytes'].describe()))
print("Downloads: " + str(data['down_bytes'].describe()))

# Check what protocols were used
protocols = data['proto'].unique()
print("Protocols used: " + str(protocols))
data['proto'].value_counts().plot(kind='bar')
plt.title('Protocols used and their frequency')
plt.xlabel('Protocol')
plt.ylabel('Occurences')
plt.savefig("./plots/protocol_frequency.png")

# Check what ports were used
ports = data['port'].unique()
print("Ports used: " + str(ports))
data['port'].value_counts().plot(kind='bar')
plt.title('Ports used and their frequency')
plt.xlabel('Port')
plt.ylabel('Occurences')
plt.savefig("./plots/port_frequency.png")

# Check what IPs were used
ips = data['dst_ip'].unique()
print("IPs used: " + str(ips))

# UDP -> 53 -> DNS
# UDP -> 443 -> QUIC
# TCP -> 443 -> HTTPS

# verify if port 53 is used only by UDP
tcp_53 = data.loc[(data['proto'] != 'udp') & (data['port']==53)]
if tcp_53.empty:
    print("Port 53 is only used by UDP")
else:
    print("Port 53 is used by TCP")

# verify if port 443 is used only by UDP
tcp_443 = data.loc[(data['proto'] != 'udp') & (data['port']==443)]
if tcp_443.empty:
    print("Port 443 is only used by UDP")
else:
    print("Port 443 is used by TCP")

#list countries that received traffic
data['dst_cc'] = data['dst_ip'].apply(lambda x: gi.country_code_by_addr(x))
countries = data['dst_cc'].unique()
print("Countries that received traffic: " + str(countries))

# Check the connections to each organization
data['dst_org'] = data['dst_ip'].apply(lambda x: gi2.org_by_addr(x))
organizations = data['dst_org'].unique()
print("Organizations: " + str(organizations))

#NET = ipaddress.IPv4Network('192.168.109.0/24')

# DNS resolution
#dns_info = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['dst_ip'])['up_bytes'].sum()
#print(dns_info)