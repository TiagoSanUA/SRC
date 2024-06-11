# Imports
import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt

# Global variables
datafile = './dataset9/test9.parquet'
gi = pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')
NET = ipaddress.IPv4Network('192.168.109.0/24')

def load_data(file_path):
    """Load data from a parquet file."""
    return pd.read_parquet(file_path)

def preprocess_data(data):
    """Preprocess the data."""
    data['timestamp'] = (data['timestamp'] / 100).astype(int)
    return data

# Load and preprocess data
data = load_data(datafile)
data = preprocess_data(data)

def dns_analysis(data):
    """Analyze DNS traffic."""
    dns_traffic = data.loc[(data['proto'] == 'udp') & (data['port'] == 53)].groupby(['dst_ip'])['up_bytes'].sum()
    return dns_traffic

def suspicious_dns_traffic(data):
    """Identify suspicious DNS traffic."""
    dns_traffic = data.loc[(data['dst_ip'].isin(['192.168.109.230', '192.168.109.225']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')
    return dns_traffic


def command_control_detection(data):
    """Detect command and control servers."""
    dns_conns = data.loc[(data['dst_ip'].isin(['192.168.103.230', '192.168.103.225']))].groupby(['src_ip'])['up_bytes'].count().sort_values()
    return dns_conns

# def c2_ratio_analysis(data):
#     """Analyze the ratio of command and control traffic."""
#     dns_conns_up = data.loc[((data['dst_ip'].isin(['192.168.103.235', '192.168.103.236'])) & (data['src_ip'].isin(['192.168.103.175', '192.168.103.137']))].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
#     dns_conns_down = data.loc[((data['dst_ip'].isin(['192.168.103.235', '192.168.103.236'])) & (data['src_ip'].isin(['192.168.103.175', '192.168.103.137']))].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
#     ratio = (dns_conns_up / dns_conns_down).sort_values()
#     return ratio


def botnet_detection(data):
    """Detect BotNet activity."""
    bpublic = data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET, axis=1)
    dfbPublic = pd.DataFrame(bpublic, columns=['pub'])
    data2 = pd.concat([data, dfbPublic], axis=1)
    addresses = data2.loc[data2['pub'] == False].groupby(['dst_ip'])['proto'].count()
    return addresses

def unique_private_contacts(data):
    """Identify unique private contacts."""
    addresses = data2.loc[data2['pub'] == False].groupby(['src_ip'])['dst_ip'].nunique()
    return addresses


def plot_traffic_distribution(traffic_data, title):
    """Plot traffic distribution."""
    traffic_data.plot(kind='bar')
    plt.title(title)
    plt.show()


def main():
    # Load and preprocess data
    data = load_data(datafile)
    data = preprocess_data(data)

    # DNS Analysis
    dns_traffic = dns_analysis(data)
    print("DNS Traffic Analysis:\n", dns_traffic)

    # Command and Control Detection
    c2_conns = command_control_detection(data)
    print("Command and Control Connections:\n", c2_conns)

    # BotNet Detection
    botnet_addresses = botnet_detection(data)
    print("BotNet Detection:\n", botnet_addresses)

    # Visualizations
    plot_traffic_distribution(dns_traffic, "DNS Traffic Distribution")
    plot_traffic_distribution(c2_conns, "Command and Control Connections")
    plot_traffic_distribution(botnet_addresses, "BotNet Detection")

if __name__ == "__main__":
    main()
