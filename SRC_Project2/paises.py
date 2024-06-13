import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 

datafile = './dataset9/data9.parquet'
testfile = './dataset9/test9.parquet'
data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)

# IP geolocation
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')

# Internal network range
NET = ipaddress.IPv4Network('192.168.109.0/24')

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
plt.savefig('./plots/connections_to_new_countries.png')
