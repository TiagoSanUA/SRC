import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 

datafile='./dataset9/test9.parquet'
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


## Innitial data analysis

# Define internal IP ranges (assuming IPv4 and internal IPs are in the private ranges)
internal_ip_ranges = ['192.168.', '10.', '172.16.', '172.17.', '172.18.', '172.19.', '172.20.', '172.21.', '172.22.', '172.23.', '172.24.', '172.25.', '172.26.', '172.27.', '172.28.', '172.29.', '172.30.', '172.31.']

# Filter data for internal IP addresses
internal_data = data[data['dst_ip'].str.startswith(tuple(internal_ip_ranges))]

# Group by destination IP and port to identify internal servers/services
internal_services = internal_data.groupby(['dst_ip', 'port']).agg({'up_bytes': 'sum', 'down_bytes': 'sum', 'dst_ip': 'count'}).rename(columns={'dst_ip': 'connections'})

# Sort by number of connections to identify most accessed internal servers/services
internal_services_sorted = internal_services.sort_values(by='connections', ascending=False)

print(internal_services_sorted)

# Calculate typical traffic volume (mean) for internal-to-internal traffic
internal_to_internal_mean = internal_services_sorted[['up_bytes', 'down_bytes']].mean()

print("Typical Internal to Internal Traffic Volume (mean):")
print(internal_to_internal_mean)

# Plot traffic volumes
plt.figure(figsize=(12, 6))

# Upload bytes by server
plt.subplot(1, 2, 1)
internal_services_sorted['up_bytes'].plot(kind='bar', color='blue')
plt.title('Upload Bytes by Internal Server')
plt.xlabel('Internal Server (IP:Port)')
plt.ylabel('Upload Bytes')

# Download bytes by server
plt.subplot(1, 2, 2)
internal_services_sorted['down_bytes'].plot(kind='bar', color='green')
plt.title('Download Bytes by Internal Server')
plt.xlabel('Internal Server (IP:Port)')
plt.ylabel('Download Bytes')

plt.tight_layout()
plt.savefig('./plots/internal_server_traffic_volumes.png')


data['timestamp'] = pd.to_datetime(data['timestamp'] / 100, unit='s').dt.strftime('%H')

# Group by hour and sum up the uploaded and downloaded bytes
hourly_data = data.groupby('timestamp').agg({'up_bytes': 'sum', 'down_bytes': 'sum'})

# Plot the hourly data transfers
plt.figure(figsize=(12, 6))
plt.plot(hourly_data.index, hourly_data['up_bytes'], label='Uploaded Bytes', marker='o')
plt.plot(hourly_data.index, hourly_data['down_bytes'], label='Downloaded Bytes', marker='o')
plt.title('Hourly Data Transfers')
plt.xlabel('Hour of Day')
plt.ylabel('Bytes')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('./plots/hourly_data_transfers.png')
plt.show()

# Find the hour with the maximum data transfer
max_up_hour = hourly_data['up_bytes'].idxmax()
max_down_hour = hourly_data['down_bytes'].idxmax()
print(f"Peak upload hour: {max_up_hour}")
print(f"Peak download hour: {max_down_hour}")


## BOTNETS

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
addresses = data2.loc[((data2['src_ip']=='192.168.109.126') | (data2['src_ip']=='192.168.109.14') | (data2['src_ip']=='192.168.109.168') | (data2['src_ip']=='192.168.109.31') | (data2['src_ip']=='192.168.109.61')) & (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
print(addresses)

# Possiveis botnets
# Output:
# src_ip
# 192.168.109.126    8
# 192.168.109.14     8
# 192.168.109.168    8
# 192.168.109.31     8
# 192.168.109.61     7


##C&C

src_ips = data[data['src_ip'].str.startswith('192.168.109.')].groupby('src_ip').size().reset_index(name='counts')
dst_ips = data[data['dst_ip'].str.startswith('192.168.109.')].groupby('dst_ip').size().reset_index(name='counts')

server_ips = src_ips.set_index('src_ip').add(dst_ips.set_index('dst_ip'), fill_value=0).reset_index().sort_values(by='counts', ascending=False).reset_index(drop=True)

src_ips_test = test[test['src_ip'].str.startswith('192.168.109.')].groupby('src_ip').size().reset_index(name='counts')
dst_ips_test = test[test['dst_ip'].str.startswith('192.168.109.')].groupby('dst_ip').size().reset_index(name='counts')

server_ips_test = src_ips_test.set_index('src_ip').add(dst_ips_test.set_index('dst_ip'), fill_value=0).reset_index().sort_values(by='counts', ascending=False).reset_index(drop=True)

# Server IPs differences (top 10 of the most used IPs on test)
top_10_server_ips_test = server_ips_test.head(10)['index'].tolist()
# Filter the normal data for the rows that have the top 10 server IPs of the test
filter_data = server_ips[server_ips['index'].isin(top_10_server_ips_test)]
# Merge to check differences
merge = pd.merge(filter_data, server_ips_test, how='inner', left_on='index', right_on='index', suffixes=('_normal', '_test'))

# Make a plot for this differences (top 10 so that there are some references of normal behaviour)
merge.plot(x='index', y=['counts_normal', 'counts_test'], kind='bar', title="IPs with more communication", xlabel="IP", ylabel="Flows")
plt.savefig('./plots/ips_with_more_communication.png')

#####

# Check which IPs normally communicate with the Server IPs and how many flows
server_ips_flows = data.loc[data['dst_ip'].isin(server_ips['index'])].groupby(['src_ip']).size().reset_index(name='counts')
#server_ips_flows = server_ips_flows.sort_values(by='src_ip', ascending=False)
print("IPs that communicate with the Server IPs and how many flows: \n" + str(server_ips_flows))

# Check which IPs communicate with the Server IPs and how many flows (test) and order them by IP
server_ips_flows_test = test.loc[test['dst_ip'].isin(server_ips_test['index'])].groupby(['src_ip']).size().reset_index(name='counts_test')
#server_ips_flows_test = server_ips_flows_test.sort_values(by='src_ip', ascending=False)
print("IPs that communicate with the \"Server IPs\" (and others with too traffic) and how many flows (test): \n" + str(server_ips_flows_test))

possible_server_attackers = pd.merge(server_ips_flows, server_ips_flows_test, how='outer', on='src_ip')

# Fill NaN with 0, since NaN represents no flows aka 0
possible_server_attackers = possible_server_attackers.fillna(0)

possible_server_attackers['increase'] = ((possible_server_attackers['counts_test'] - possible_server_attackers['counts']) / possible_server_attackers['counts']) * 100
possible_server_attackers.sort_values(by='increase', ascending=False, inplace=True)
print("Increase of communication in percentage to the servers from normal to test: \n" + str(possible_server_attackers))


server_attackers_ips = possible_server_attackers.loc[((possible_server_attackers['increase'] > 250) | (possible_server_attackers['increase'] == np.inf)) & (possible_server_attackers['counts'] > 50)]
print("Possible server attackers IPs that have an increase over 250% or inf, have more than 50 flows to the servers: \n" + str(server_attackers_ips))

# Graph with the increase of communication in percentage to the servers from normal to test
server_attackers_ips.plot(x='src_ip', y='increase', kind='bar', title="Increase of communication in percentage to the servers from normal to test", xlabel="IPs with 150% increase of comms to the servers and at least 50 flows to the servers", ylabel="Increase (%)")
plt.savefig('./plots/increase_communication_to_servers.png')


## EXFILTRATION

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


## WEIRD COUNTRIES

# Geolocalization of destination IP addresses for inside-to-inside communications
data['dst_cc'] = data['dst_ip'].apply(lambda x: gi.country_code_by_addr(x) if ipaddress.IPv4Address(x) not in NET else None)
data_countries = data['dst_cc'].dropna().unique()
#print number of counntries
print(f"Number of countries in data: {len(data_countries)}")


# Geolocalization of destination IP addresses for inside-to-outside communications
test['dst_cc'] = test['dst_ip'].apply(lambda x: gi.country_code_by_addr(x) if ipaddress.IPv4Address(x) not in NET else None)
test_countries = test['dst_cc'].dropna().unique()

# Identify new countries in the test data
new_countries = set(test_countries) - set(data_countries)
print(f"New countries in test data: {new_countries}")

# List the IPs responsible for the new countries
new_country_ips = test[test['dst_cc'].isin(new_countries)][['src_ip', 'dst_ip', 'dst_cc']].drop_duplicates()
print("IPs responsible for new countries:")
print(new_country_ips)

unique_new_country_ips = test[test['dst_cc'].isin(new_countries)][['src_ip', 'dst_cc']].drop_duplicates()

# Save the results to a CSV file if needed
new_country_ips.to_csv('./new_country_ips.csv', index=False)
unique_src_ips = new_country_ips['src_ip'].unique()
print("Unique source IPs responsible for new countries:")
print(unique_src_ips)

new_country_ips['dst_cc'].value_counts().plot(kind='bar', title='Connections to New Countries', xlabel='Country', ylabel='Count')
plt.savefig('./plots/connections_to_new_countries.png')# Geolocalization of destination IP addresses for inside-to-inside communications
data['dst_cc'] = data['dst_ip'].apply(lambda x: gi.country_code_by_addr(x) if ipaddress.IPv4Address(x) not in NET else None)
data_countries = data['dst_cc'].dropna().unique()
#print number of counntries
print(f"Number of countries in data: {len(data_countries)}")


# Geolocalization of destination IP addresses for inside-to-outside communications
test['dst_cc'] = test['dst_ip'].apply(lambda x: gi.country_code_by_addr(x) if ipaddress.IPv4Address(x) not in NET else None)
test_countries = test['dst_cc'].dropna().unique()

# Identify new countries in the test data
new_countries = set(test_countries) - set(data_countries)
print(f"New countries in test data: {new_countries}")

# List the IPs responsible for the new countries
new_country_ips = test[test['dst_cc'].isin(new_countries)][['src_ip', 'dst_ip', 'dst_cc']].drop_duplicates()
print("IPs responsible for new countries:")
print(new_country_ips)

unique_new_country_ips = test[test['dst_cc'].isin(new_countries)][['src_ip', 'dst_cc']].drop_duplicates()

# Save the results to a CSV file if needed
new_country_ips.to_csv('./new_country_ips.csv', index=False)
unique_src_ips = new_country_ips['src_ip'].unique()
print("Unique source IPs responsible for new countries:")
print(unique_src_ips)

new_country_ips['dst_cc'].value_counts().plot(kind='bar', title='Connections to New Countries', xlabel='Country', ylabel='Count')
plt.savefig('./plots/connections_to_new_countries.png')

