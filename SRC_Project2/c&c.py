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

# dns_conns = data.loc[(data['dst_ip']=='192.168.109.227') | (data['dst_ip']=='192.168.109.224')].groupby(['src_ip'])['up_bytes'].count().sort_values()
# print(dns_conns[-15:])


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
merge.plot(x='index', y=['counts_normal', 'counts_test'], kind='bar', title="IPs (normally, Server IPs) with more communication", xlabel="IP", ylabel="Flows")
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