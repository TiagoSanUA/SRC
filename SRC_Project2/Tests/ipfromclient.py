import numpy as np
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt 
import pandas as pd
import ipaddress

# Functions
def bytes_to_mb(bytes):
    return bytes / 1000000

def ts_to_hours(ts):
    ts = ts / 100
    hours, rem = divmod(ts, 3600)
    minutes, secs = divmod(rem, 60)
    return "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(secs))

# Path to your data file
datafile = './dataset9/test9.parquet'

# Read parquet data files
data = pd.read_parquet(datafile)
print(data.head())
print("Uploads: " + str(data['up_bytes'].describe()))
print("\nDownloads: " + str(data['down_bytes'].describe()))

# Check what protocols were used
protocols = data['proto'].unique()
print("Protocols used: " + str(protocols))
data['proto'].value_counts().plot(kind='bar')
plt.title('Protocols used and their frequency')
plt.xlabel('Protocol')
plt.ylabel('Occurrences')
# plt.show()

# Check what ports were used
ports = data['port'].unique()
print("Ports used: " + str(ports))
data['port'].value_counts().plot(kind='bar')
plt.title('Ports used and their frequency')
plt.xlabel('Port')
plt.ylabel('Occurrences')
# plt.show()

# Check what IPs were used
ips = data['dst_ip'].unique()
print("IPs used: " + str(ips))

# n ha nada nas portas 53, 443
# Verify that port 53 only has UDP connections
tcp53 = data.loc[(data['port'] == 53) & (data['proto'] != 'tcp')]
if tcp53.empty:
    print("No TCP connections on port 53")
else:
    print("TCP connections on port 53: \n" + str(tcp53))

# Verify that port 443 only has TCP connections
udp443 = data.loc[(data['port'] == 443) & (data['proto'] != 'udp')]
if udp443.empty:
    print("No UDP connections on port 443")
else:
    print("UDP connections on port 443: \n" + str(udp443))

# Count the number of occurrences of each unique (src_ip, dst_ip) pair
ip_links = data.groupby(['src_ip', 'dst_ip']).size().reset_index(name='count')

# Display the result
print("Number of connections: " + str(len(ip_links)))
print("Diferent connections: \n" + str(ip_links))

ip_links_sorted = ip_links.sort_values(by='count', ascending=False)
print("Different connections sorted by count:\n", ip_links_sorted)


# Add the country code to flow data
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
data['dst_cc'] = data['dst_ip'].apply(lambda x: gi.country_code_by_addr(x))

# Check the connections to each country
countries = data['dst_cc'].unique()
print("Number of countries " + str(len(countries)))
print("Countries: " + str(countries))

# Add the organization to flow data
gi2 = pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')
data['dst_org'] = data['dst_ip'].apply(lambda x: gi2.org_by_addr(x))

# Check the connections to each organization
organizations = data['dst_org'].unique()
print("Organizations: " + str(organizations))

# Timestamp to hours
data['ts_in_hours'] = data['timestamp'].apply(lambda x: ts_to_hours(x))

# Is destination IPv4 a public address?
NET = ipaddress.IPv4Network('192.168.109.0/24')
bpublic = data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET, axis=1)

# Add source IP geolocation (client location)
data['src_cc'] = data['src_ip'].apply(lambda x: gi.country_code_by_addr(x))

# Count the occurrences of each client location
client_location_counts = data['src_cc'].value_counts().reset_index()
client_location_counts.columns = ['location', 'count']

print("Client Locations:")
print(client_location_counts.head())

# Plot the client locations
client_location_counts.plot(kind='bar', x='location', y='count')
plt.title('Client Locations and their Frequency')
plt.xlabel('Location')
plt.ylabel('Occurrences')
# plt.show()

# Concat data with cc and org on the column
data_extra_info = pd.concat([data, data['dst_cc'], data['dst_org'], data['ts_in_hours']], axis=1)

