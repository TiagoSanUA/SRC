import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 


# Load data
datafile = './dataset9/data9.parquet'
testfile = './dataset9/test9.parquet'
data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)

# Define internal network
NET = ipaddress.IPv4Network('192.168.109.0/24')

# Initialize GeoIP databases
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')

# Define functions for SIEM rules
def calculate_normal_stats(data):
    # Group and count flows
    normal_stat = data.groupby(['src_ip']).size().reset_index(name='flows')
    
    # Calculate flows to different destinations
    normal_stat['flows_to_internet'] = data.loc[~data['dst_ip'].apply(lambda x: ipaddress.ip_address(x).is_private)].groupby(['src_ip']).size().reset_index(name='flows_to_internet')['flows_to_internet']
    normal_stat['flows_to_private_ips'] = data.loc[data['dst_ip'].apply(lambda x: ipaddress.ip_address(x).is_private)].groupby(['src_ip']).size().reset_index(name='flows_to_private_ips')['flows_to_private_ips']
    
    # Calculate byte statistics
    normal_stat['total_up_bytes'] = data.groupby(['src_ip'])['up_bytes'].sum().reset_index(name='up_bytes')['up_bytes']
    normal_stat['total_down_bytes'] = data.groupby(['src_ip'])['down_bytes'].sum().reset_index(name='down_bytes')['down_bytes']
    normal_stat['up_bytes_per_flow'] = normal_stat['total_up_bytes'] / normal_stat['flows']
    normal_stat['down_bytes_per_flow'] = normal_stat['total_down_bytes'] / normal_stat['flows']
    
    # Protocol-specific flow statistics
    normal_stat['% udp_flows'] = data.loc[data['proto'] == 'udp'].groupby(['src_ip']).size().reset_index(name='udp_flows')['udp_flows'] / normal_stat['flows']
    normal_stat['% tcp_flows'] = data.loc[data['proto'] == 'tcp'].groupby(['src_ip']).size().reset_index(name='tcp_flows')['tcp_flows'] / normal_stat['flows']
    
    return normal_stat

def detect_high_volume_uploads(normal_up_down, test_up_down, threshold=1.5):
    normal_avg_up_bytes = normal_up_down['avg_up_bytes'].mean()
    test_avg_up_bytes = test_up_down['avg_up_bytes'].mean()
    return test_avg_up_bytes > (normal_avg_up_bytes * threshold)

def detect_high_volume_downloads(normal_up_down, test_up_down, threshold=1.5):
    normal_avg_down_bytes = normal_up_down['avg_down_bytes'].mean()
    test_avg_down_bytes = test_up_down['avg_down_bytes'].mean()
    return test_avg_down_bytes > (normal_avg_down_bytes * threshold)

def detect_high_number_connections_to_external_ips(test_flows, threshold=37):
    return test_flows['counts'].sum() > threshold

def detect_unusual_access_to_multiple_countries(test, threshold=5):
    test['country_code'] = test['dst_ip'].apply(lambda y: gi.country_code_by_addr(y) if ipaddress.IPv4Address(y) not in NET else '')
    unique_countries = test[test['country_code'] != ''].groupby('src_ip')['country_code'].nunique()
    return unique_countries[unique_countries > threshold].index.tolist()

def detect_large_data_transfers_to_specific_countries(test, countries=['CN', 'RU'], threshold=1000000):
    test['country_code'] = test['dst_ip'].apply(lambda y: gi.country_code_by_addr(y) if ipaddress.IPv4Address(y) not in NET else '')
    data_transfers = test[test['country_code'].isin(countries)].groupby('src_ip')['up_bytes'].sum()
    return data_transfers[data_transfers > threshold].index.tolist()

# Prepare data for normal and test periods
normal_up_down = data.loc[~data['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
normal_flows = data.loc[~data['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).size().reset_index(name='counts')
test_up_down = test.loc[~test['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
test_flows = test.loc[~test['dst_ip'].str.startswith('192.168.109.')].groupby(['src_ip']).size().reset_index(name='counts')

# Calculate average bytes per flow
normal_up_down = pd.merge(normal_up_down, normal_flows, on='src_ip', how='inner')
normal_up_down['avg_up_bytes'] = normal_up_down['up_bytes'] / normal_up_down['counts']
normal_up_down['avg_down_bytes'] = normal_up_down['down_bytes'] / normal_up_down['counts']
test_up_down = pd.merge(test_up_down, test_flows, on='src_ip', how='inner')
test_up_down['avg_up_bytes'] = test_up_down['up_bytes'] / test_up_down['counts']
test_up_down['avg_down_bytes'] = test_up_down['down_bytes'] / test_up_down['counts']

# Apply SIEM rules
normal_stats = calculate_normal_stats(data)
high_uploads_detected = detect_high_volume_uploads(normal_up_down, test_up_down)
high_downloads_detected = detect_high_volume_downloads(normal_up_down, test_up_down)
high_connections_to_external_detected = detect_high_number_connections_to_external_ips(test_flows)
unusual_country_access_ips = detect_unusual_access_to_multiple_countries(test)
large_data_transfers_ips = detect_large_data_transfers_to_specific_countries(test)

# Print results
print("High Volume Uploads Detected:", high_uploads_detected)
print("High Volume Downloads Detected:", high_downloads_detected)
print("High Number of Connections to External IPs Detected:", high_connections_to_external_detected)
print("IPs with Unusual Access to Multiple Countries:", unusual_country_access_ips)
print("IPs with Large Data Transfers to Specific Countries:", large_data_transfers_ips)

# Visualize exfiltration candidates
exfilt_up_down = test.loc[(test['src_ip'].isin(unusual_country_access_ips)) & (~test['dst_ip'].str.startswith('192.168.109.'))].groupby(['src_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
exfilt_up_down.plot(x='src_ip', y=['up_bytes', 'down_bytes'], kind='bar', title="Exfiltration", xlabel="IP", ylabel="Bytes")