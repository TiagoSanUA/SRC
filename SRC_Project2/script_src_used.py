# pip install pygeoip
# pip install fastparquet 
# pip install dnspython
import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import pygeoip
import datetime
import matplotlib.pyplot as plt 

datafile='./dataset3/test3.parquet'

### IP geolocalization
gi=pygeoip.GeoIP('./GeoIP_DBs/GeoIP.dat')
gi2=pygeoip.GeoIP('./GeoIP_DBs/GeoIPASNum.dat')
""" addr='193.136.73.21'
cc=gi.country_code_by_addr(addr)
org=gi2.org_by_addr(addr)
print(cc,org) """

### DNS resolution
""" addr=dns.resolver.resolve("www.ua.pt", 'A')
for a in addr:
    print(a) """
    
### Reverse DNS resolution    
""" name=dns.reversename.from_address("193.136.172.20")
addr=dns.resolver.resolve(name, 'PTR')
for a in addr:
    print(a) """

### Read parquet data files
data=pd.read_parquet(datafile)	
#print(data.to_string())

#Just the UDP flows
""" udpF=data.loc[data['proto']=='udp'] """

#Number of UDP flows for each source IP
""" nudpF=data.loc[data['proto']=='udp'].groupby(['src_ip'])['up_bytes'].count()

nudpF=data.loc[data['port']==443].groupby(['src_ip'])['up_bytes'].sum().sort_values() """


#Number of UDP flows to port 443, for each source IP
""" nudpF443=data.loc[(data['proto']=='udp')&(data['port']==443)].groupby(['src_ip'])['up_bytes'].count() """

#Average number of downloaded bytes, per flow, for each source IP
""" avgUp=data.groupby(['src_ip'])['down_bytes'].mean()
print(avgUp.sort_values()) """


#Total uploaded bytes to destination port 443, for each source IP, ordered from larger amount to lowest amount
""" upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False) """

#Histogram of the total uploaded bytes to destination port 443, by source IP
#upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().hist()
#plt.show()

#Is destination IPv4 a public address?
NET=ipaddress.IPv4Network('192.168.103.0/24')
""" bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1) """

#Geolocalization of public destination adddress
""" cc=data[bpublic]['dst_ip'].apply(lambda y:gi.country_code_by_addr(y)).to_frame(name='cc') """



#########################################################################################################
# ver qual é o dns(udp e 53), agrupar o destino e ver quem fez mais upload

""" dns1 = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['dst_ip'])['up_bytes'].sum()
print(dns1) """

#      dst ip         upload
# 192.168.103.235    18994071
# 192.168.103.236    19558699

#      dst ip      no conns
#192.168.103.235    95136
#192.168.103.236    97511

#########################################################################################################
# ver quantidade de up/down para dns

""" dns_traffic = data.loc[(data['dst_ip']=='192.168.103.235') | (data['dst_ip']=='192.168.103.236')].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')
print(dns_traffic[-20:]) """

#these are suspicious:
#      src ip          up         down
# 192.168.103.78     297570      671101
# 192.168.103.107    349371      812688
# 192.168.103.175   5816880    13240471
# 192.168.103.137  10191327    23322294

#########################################################################################################







##########################################################################################################
#                                       COMMAND & CONTROL                                                #
##########################################################################################################
print("\n\n\nCOMMAND & CONTROL\n")
# numero de conexoes ao servidor DNS, por cada IP da rede
""" dns_conns = data.loc[(data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')].groupby(['src_ip'])['up_bytes'].count().sort_values()
print(dns_conns[-15:]) """

# src                    dst         no conns
# 192.168.103.175  192.168.103.236    14462
#                  192.168.103.235    14630
# 192.168.103.137  192.168.103.235    24641
#                  192.168.103.236    26086

#src                no conns
# 192.168.103.107     1738
# 192.168.103.175    29092
# 192.168.103.137    50727

#########################################################################################################
# ratio destas conexoes com o servidor DNS

""" dns_conns_up = data.loc[((data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
dns_conns_down = data.loc[((data['dst_ip']=='192.168.103.236') | (data['dst_ip']=='192.168.103.235')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
ratio =(dns_conns_up/dns_conns_down).sort_values()
print(ratio) """

#ratio normal 0.43, conclui se que são command and control

#########################################################################################################
# ratio destas conexoes com o servidor mail/web

""" dns_conns_up = data.loc[((data['dst_ip']=='192.168.103.222') | (data['dst_ip']=='192.168.103.238')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
dns_conns_down = data.loc[((data['dst_ip']=='192.168.103.222') | (data['dst_ip']=='192.168.103.238')) & ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
ratio =(dns_conns_up/dns_conns_down).sort_values()
print(ratio) """
#ratio normal 0.11

#########################################################################################################
# ratio destas conexoes 

""" dns_conns_up = data.loc[((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['up_bytes'].sum()
dns_conns_down = data.loc[ ((data['src_ip']=='192.168.103.175') | (data['src_ip']=='192.168.103.137'))].groupby(['src_ip'])['down_bytes'].sum()
ratio =(dns_conns_up/dns_conns_down).sort_values()
print(ratio) """

#ratio normal 0.11









##########################################################################################################
#                                              BOTNETS                                                   #
##########################################################################################################
# preparar tabela
print("\n\n\nBOTNETS\n")
""" dfbPublic = pd.DataFrame(bpublic,columns = ['pub'])
data2 = pd.concat([data, dfbPublic], axis=1) """


# destinos privados que foram contactados
""" addresses = data2.loc[(data2['pub']==False)].groupby(['dst_ip'])['proto'].count()
print(addresses) """

#       dst_ip       no conns
# 192.168.103.177      146
# 192.168.103.54       276
# 192.168.103.58       227
# 192.168.103.222    82085 (server)
# 192.168.103.235    95136 (server)
# 192.168.103.236    97511 (server)
# 192.168.103.238    82142 (server)

##########################################################################################################
# numero de conexoes feitas para ips privados(unique)
""" addresses = data2.loc[ (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
print(addresses.sort_values()) """

# src ip           no conns
# 192.168.103.163    4
# 192.168.103.154    4
# 192.168.103.58     6
# 192.168.103.54     6
# 192.168.103.177    6


##########################################################################################################
# ips locais que estas as maquinas comprometidas contactaram

""" addresses = data2.loc[((data2['src_ip']=='192.168.103.177') | (data2['src_ip']=='192.168.103.54') | (data2['src_ip']=='192.168.103.58')) & (data2['pub']==False) ].groupby(['src_ip'])['dst_ip'].unique()
print(addresses) """

#ouput: mostra botnets e 4 servidores

##########################################################################################################
# numero de ips locais que estas as maquinas comprometidas contactaram

""" addresses = data2.loc[((data2['src_ip']=='192.168.103.177') | (data2['src_ip']=='192.168.103.54') | (data2['src_ip']=='192.168.103.58')) & (data2['pub']==False)].groupby(['src_ip'])['dst_ip'].nunique()
print(addresses) """

#   src ip          no conns
# 192.168.103.177    6
# 192.168.103.54     6
# 192.168.103.58     6







##########################################################################################################
#                                           GOOGLE                                                       #
##########################################################################################################
print("\n\n\nGOOGLE\n")
# uploads/downloads para google drive: proto=udp port=443

""" google = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum().sort_values(by='up_bytes')
print(google) """

#     src ip              up         down
# 192.168.103.43     4202372468    82489777
# 192.168.103.47     6703378051   115591907


##########################################################################################################
# ratios para google drive: proto=udp port=443

""" traffic_up = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['up_bytes'].sum()
traffic_down = data.loc[(data['proto']=='udp') & (data['port']==443)].groupby(['src_ip'])['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values()[-5:]) """

#     src ip          ratio
# 192.168.103.47     57.991759
# 192.168.103.43     50.944161

###########################################################################################################
# numero de conexoes e uplaod feito por 192.168.103.43 e 192.168.103.47, para o google drive 

""" traffic = data.loc[(data['proto']=='udp') & (data['port']==443) & ((data['src_ip']=='192.168.103.43') | (data['src_ip']=='192.168.103.47'))].groupby(['src_ip','dst_ip']).agg({'dst_ip': 'count', 'up_bytes': 'sum'})
print(traffic.sort_values(by='up_bytes')[-3:]) """

#       src           dst          conns   up ammount   
#192.168.103.43   142.250.184.157      26  4202029935
#192.168.103.47   142.250.184.247      39  6702988331

###########################################################################################################
# maneira facil de ver o volume de dados de upload das maquinas comprometidas para o google drive

""" print(data.loc[data['src_ip'].isin(['192.168.103.47', '192.168.103.43'])].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum().sort_values()[-15:])
"""







##########################################################################################################
#                                           TWITTER                                                      #
##########################################################################################################
print("\n\n\nTWITTER\n")
# ratios de upload/download para tcp
""" base = data.loc[(data['proto']=='tcp')].groupby(['src_ip'])
traffic_up = base['up_bytes'].sum()
traffic_down = base['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values())
 """
#    ip source         ratio
# 192.168.103.79     0.118350
# 192.168.103.110    0.376267
# 192.168.103.207    0.392411

##########################################################################################################
# numero de comunicacoes que estas maquinas fizeram para cada destino

""" conns = data2.loc[data2['src_ip'].isin(['192.168.103.110','192.168.103.207']) & (data2['pub']==True)].groupby(['src_ip', 'dst_ip'])['port'].count()
print(conns.sort_values()[-30:]) """



##########################################################################################################
# ratio de bytes para cada destino
""" base = data.loc[data['src_ip'].isin(['192.168.103.110','192.168.103.207'])].groupby(['src_ip', 'dst_ip'])
traffic_up = base['up_bytes'].sum()
traffic_down = base['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values()[-15:]) """

#   ip source          ip dst          ratio
# 192.168.103.207  104.244.42.1       29.011447
# 192.168.103.110  104.244.42.1       29.675233

##########################################################################################################
#  detalhes das conexoes das maquinas que exfiltraram para o twitter (numero de ligacoes e tamanho de upload)

""" conns = data.loc[data['src_ip'].isin(['192.168.103.110','192.168.103.207']) & data['dst_ip'].isin(['104.244.42.1'])].groupby(['src_ip','dst_ip']).agg({'dst_ip': 'count', 'up_bytes': 'sum'})
print(conns) """

#   src ip          dst ip      no conns    up ammount
# 192.168.103.110 104.244.42.1     157   72355696
# 192.168.103.207 104.244.42.1     319  143941917









##########################################################################################################
#                                           exfiltracao DNS                                              #
##########################################################################################################
print("\n\n\nexfiltracao DNS\n")
# se tem um ratio de comunicacao 0.41-0.45 verificar se falou muito com o dns
""" base = data.loc[(data['proto']=='udp')& (data['port']==53)].groupby(['src_ip'])
traffic_up = base['up_bytes'].sum()
traffic_down = base['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(traffic_up.sort_values()) """

#########################################################################################################
# ver racios up/down para os web/mail servidores internos
""" base = data.loc[(data['dst_ip'].isin(['192.168.103.222', '192.168.103.238']))].groupby(['src_ip'])
traffic_up = base['up_bytes'].sum()
traffic_down = base['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values()) """

# nada estranho, racios no intervalo [0.11-0.12] menos este:
# 192.168.103.37     0.130477

#########################################################################################################
# ver numero de conexoes para servidores internos
""" base = data.loc[(data['dst_ip'].isin([ '192.168.103.238', '192.168.103.222']))].groupby(['src_ip'])
conns = base['dst_ip'].count()
print(conns.sort_values()) """

# conexoes com o server dns:
# 192.168.103.78      1491
# 192.168.103.107     1738

# conexoes com o server web/mail:
# 192.168.103.78     2271

#########################################################################################################   
#ver o que o 37, 78 e o 107 anda a fazer: ratio

""" bpublic=data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) not in NET,axis=1)
dfbPublic = pd.DataFrame(bpublic,columns = ['bpublic'])
data2 = pd.concat([data, dfbPublic], axis=1)

traffic_up = data2.loc[(data2['src_ip'].isin(['192.168.103.78', '192.168.103.107', '192.168.103.37'])) & (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['up_bytes'].sum()
traffic_down = data2.loc[(data2['src_ip'].isin(['192.168.103.78', '192.168.103.107', '192.168.103.37'])) & (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])['down_bytes'].sum()
ratio = traffic_up/traffic_down
print(ratio.sort_values()) """

# ratios perfeitos

#########################################################################################################   
#ver o que o 37, 78 e o 107 anda a fazer: volume de dados
""" dados = data2.loc[(data2['src_ip'].isin(['192.168.103.78', '192.168.103.107', '192.168.103.37'])) & (data2['bpublic']==False)].groupby(['src_ip', 'dst_ip'])[['up_bytes', 'down_bytes']].sum()
print(dados) """


# .78 tem um volume de dados ligeiramente grande no mail/web server

#############################################################################################################
# tamanho medio das conexoes para os servidores dns
base = data.loc[(data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].mean()
print(base.sort_values(by='up_bytes'))

#############################################################################################################
#


# soma de bytes das conexoes com o dns
base = data.loc[(data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])[['up_bytes', 'down_bytes']].sum()
print(base.sort_values('up_bytes')[-50:])

# up bytes para o DNS
# 192.168.103.175     5816880
# 192.168.103.137    10191327

#ver com quem o 78 e 107 comunicaram
base = data.loc[(data['src_ip'].isin(['192.168.103.107', '192.168.103.78']))].groupby(['dst_ip'])[['up_bytes']].count()
print(base.sort_values(by='up_bytes')[-30:])

# muitos pedidos foram para aqui
# 142.250.200.68
# 213.13.146.142
# 172.217.17.14
# 157.240.212.35
# 157.240.212.174

addr2=['142.250.200.68', '213.13.146.142','172.217.17.14','157.240.212.35', '157.240.212.174', '216.58.215.131', '88.157.217.145']
for addr in addr2:
    cc=gi.country_code_by_addr(addr)
    org=gi2.org_by_addr(addr)
    name=dns.reversename.from_address("193.136.172.20")
    print(cc,org,"-", name)

base = data.loc[(data['proto']=='udp') & (data['port']==53)].groupby(['src_ip'])[['up_bytes', 'down_bytes']].count()
print(base.sort_values(by='up_bytes')[-30:])






##########################################################################################################
#                                           timestamps                                                   #
##########################################################################################################
print("\n\n\nTIMESTAMPS\n")

# upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().hist()
# plt.show()

""" data['timestamp'] = pd.to_datetime(data['timestamp'])
hist_data = data.groupby('timestamp').size().hist()
plt.xlabel('Timestamp')
plt.ylabel('Number of Entries')
plt.title('Histogram of Entries by Timestamp') """

""" data['timestamp'] = pd.to_datetime(data['timestamp'])
upS=data.groupby(['timestamp'])['up_bytes'].sum().plot.hist() """

""" upS=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().hist()
plt.show()
 """

# Group the data by 'timestamp' 
""" hist_data = data.groupby(['timestamp'])['up_bytes'].sum()
print(hist_data[-60:].to_string()) """

##########################################################################################################
# get last entries

""" print(data)
print(data.sort_values(by="index")) """

##########################################################################################################
# format time
data['timestamp'] = (data['timestamp'] / 100).astype(int) 
""" data['time'] = pd.to_datetime(data['timestamp'], unit='s').dt.strftime('%H:%M') """

##########################################################################################################
# get ammount of up/down  per minute
""" base = data.groupby(['time'])[['up_bytes', 'down_bytes']].sum()
print(base.sort_values('time')[500:].to_string()) """

# hour     up            down
# 00:00  125966112     1876540
# 00:20  225117179     2992655
# 07:28  135292181    31626391
# 07:48  285298411    66498156
# 23:40  203326406     2172356

##########################################################################################################
# get ammount of connections  per minute
""" base = data.groupby(['time'])[['up_bytes']].sum()
print(base.sort_values(by='up_bytes')[-20:]) """

##########################################################################################################
# get ammount of traffic  per timestamp
""" base = data.groupby(['timestamp'])[['up_bytes']].sum()
print(base.sort_values(by='up_bytes')[-20:]) """

#
# 85241      203326406      23:40
# 87642      225117179      00:20

##########################################################################################################
# get connections from ~22:20 to ~00:20
""" base = data.loc[(data['timestamp']>80000) & (data['timestamp']<90000)]
print(base.sort_values(by='up_bytes')[-30:].to_string()) """

#         timestamp          src_ip  ...   up_bytes down_bytes
# index                              ...       
# 946563      80442  192.168.103.47  ...   74739315     992272   22:20
# 946564      81644  192.168.103.47  ...   89077581    1321441   22:40   
# 946565      82843  192.168.103.47  ...  165748166    2160184   23:00
# 946566      84041  192.168.103.47  ...  117361867    1694618   23:20
# 946567      85241  192.168.103.47  ...  203326406    2172356   23:40
# 946568      86442  192.168.103.47  ...  125966112    1876540   00:00
# 946569      87642  192.168.103.47  ...  225117179    2992655   00:20


##########################################################################################################
# get connections from ~07:25 to ~07:50
""" base = data.loc[(data['timestamp']>26700) & (data['timestamp']<28200)]
print(base.sort_values(by='up_bytes')[-50:]) """

#         timestamp          src_ip                            ...   up_bytes down_bytes
# index                                  
# 851380      27859   192.168.103.32     13.107.42.32   tcp   443     102153     5858458
# 946570      26922   192.168.103.43  142.250.184.157   udp   443  132110621     2878073
# 946571      28123   192.168.103.43  142.250.184.157   udp   443  278378153     3329167



##########################################################################################################
# get ammount of conns  per timestamp
""" base = data.groupby(['timestamp'])[['up_bytes']].count()
print(base.sort_values(by='up_bytes')[-20:]) """

# nada se conclui



# hour          up          down 
""" 23:35:01      18190      128682
23:35:02      12305      129903
23:35:03       7305       72981
23:35:04      11982      145782
23:40:41  203326406     2172356 """



##########################################################################################################
#                                           paises                                                      #
##########################################################################################################

"""         
new countries contacted :
['KR', 'MM', 'ID', 'NZ', 'TH', 'NA', 'RU', 'JP', 'ES', 'MY', 'UA', 
 'IR', 'PL', 'IL', 'IE', 'SE', 'FR', 'FI', 'TW', 'CA', 'KG', 'LV', 
 'CZ', 'KW', 'GL', 'LB', 'GR', 'GE', 'TR', 'CY', 'MX', 'UZ', 'KZ', 
 'SI', 'LT', 'AT', 'MD', 'EE', 'KH', 'RO', 'PA', 'PH']



      ip          Countries Contacted
192.168.103.34  11
192.168.103.67  18
192.168.103.160 16


      ip         up to strange countries
192.168.103.34       6 924 452
192.168.103.67      12 184 075
192.168.103.160      6 090 109

 """


print("finish")


# command and control dns (muitas ligacoes com os servers dns e ratio normal de bytes)
# 192.168.103.107

base = data.loc[(data['src_ip'].isin(['192.168.103.175', '192.168.103.137'])) & (data['dst_ip'].isin(['192.168.103.235', '192.168.103.236']))].groupby(['src_ip'])
up = base['up_bytes'].sum()
print(up.sort_values().to_string())

""" base = data.groupby(['dst_ip'])['up_bytes'].sum()
print(base.sort_values()) """

""" result = data.groupby('dst_ip').agg({'up_bytes': 'sum', 'src_ip': lambda x: list(set(x))})
result = result.sort_values('up_bytes', ascending=True)
last_two_values = result.tail(2)

print(set(last_two_values['src_ip'].tolist())) """

"""
anomal=['CN', 'KR', 'HK', 'MM', 'IN', 'ID', 'US', 'NZ', 'TH', 'AU', 'NA', 'RU', 'NL', 'JP',
 'SG', 'ES', 'DE', 'MY', 'GB', 'UA', 'IR', 'BR', 'PL', 'IT', 'IL', 'ZA', 'IE', 'AE',
 'SE', 'CH', 'FR', 'NO', 'FI', 'OM', 'TW', 'DK', 'CA', 'PT', 'KG', 'BH', 'LV', 'CZ',
 'KW', 'GL', 'BE', 'LB', 'GR', 'GE', 'TR', 'CY', 'MX', 'AP', 'UZ', 'KZ', 'SI', 'LT',
 'AT', 'MD', 'EE', 'KH', 'SA', 'RO', 'CL', 'PA', 'PH']

nomal = ['US', 'NA', 'NL', 'AU', 'IE', 'SG', 'CA', 'IN', 'ES', 'GB', 'BR', 'KR', 'MY', 'IT', 'IL', 'JP',  'HK', 'AE', 'DE', 'SE', 'CN', 'BH', 'ID', 'FR',  'PL', 'OM', 'DK', 'PT','ZA', 'CH', 'BE', 'AP',  'SA', 'NO', 'CL']

for a in anomal:
    if a in nomal:
        anomal.remove(a)

print(anomal)

print(len(abc)) """
