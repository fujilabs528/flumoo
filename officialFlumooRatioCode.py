import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys
import psycopg2
import csv
import datetime
import pickle
import re
import matplotlib.pyplot as plt
import locale
from locale import atof

###########################################################
p = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB',
     'ARE', 'ALXN', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP',
     'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL',
     'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC',
     'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR',
     'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE',
     'CNC', 'CNP', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG',
     'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT',
     'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN',
     'DXCM', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK',
     'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX',
     'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX',
     'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN',
     'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI',
     'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM',
     'HPQ', 'HUM', 'HBAN', 'HII', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG',
     'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU',
     'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG',
     'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX',
     'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU',
     'MSFT', 'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP',
     'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH',
     'NRG', 'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX',
     'PAYC', 'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG',
     'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O',
     'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX',
     'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB',
     'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TDY', 'TFX', 'TXN', 'TXT', 'TMO', 'TIF',
     'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL',
     'UNH', 'UPS', 'URI', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VIAC', 'V', 'VNO',
     'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WU', 'WRK', 'WY',
     'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']
#variable = ["alpha","beta","charlie","delta","echo","foxtrot","golf","hotel","india","juliet","kilo","oscar","mike", "november"]
#variable = ["papa","quebec", "uniform", "sierra", "tango", "whiskey","yankees", "xray", "zulu"]

engine = create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
###################################################################################
alpha = pd.read_sql_table("equity", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

equitylist=[]
for r in fatlist:
    equitylist.append(r[1])
#print(equitylist)
###################################################################################
alpha = pd.read_sql_table("healthratio", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

healthratiolist=[]
for r in fatlist:
    healthratiolist.append(r[1])
#print(healthratiolist)
###################################################################################
alpha = pd.read_sql_table("endCashPosition", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

endCashPositionlist=[]
for r in fatlist:
    endCashPositionlist.append(r[1])
#print(endCashPositionlist)
###################################################################################
alpha = pd.read_sql_table("operatingCashFlow", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

operatingCashFlowlist=[]
for r in fatlist:
    operatingCashFlowlist.append(r[1])
#print(operatingCashFlowlist)
###################################################################################

alpha = pd.read_sql_table("dominanceratio", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

dominanceratiolist=[]
for r in fatlist:
    dominanceratiolist.append(r[1])
#print(dominanceratiolist)
###################################################################################
alpha = pd.read_sql_table("orgleanratio", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

orgleanratiolist=[]
for r in fatlist:
    orgleanratiolist.append(r[1])
#print(orgleanratiolist)
###################################################################################
alpha = pd.read_sql_table("top2bottomratio", engine)
yankees = {}
e= 0
for x in alpha["tickername"]:
        yankees[x]=e
        e=e+1
#print(yankees)
#print(yankees.values([9]))

fatlist=sorted(yankees.items(),key= lambda x:x[0])
#print(fatlist)
#print(type(w))

top2bottomratiolist=[]
for r in fatlist:
    top2bottomratiolist.append(r[1])
#print(top2bottomratiolist)
###################################################################################
metslist=[]
for i in range(505):
    w=equitylist[i]+healthratiolist[i]+endCashPositionlist[i]+operatingCashFlowlist[i]+dominanceratiolist[i]+orgleanratiolist[i]+top2bottomratiolist[i]
    metslist.append(w)
#print(metslist)

angelslist= []
for v in fatlist:
    angelslist.append(v[0])
#print(angelslist)
###################################################################################
metslistDF= pd.DataFrame()
metslistDF["totalvalue"]=metslist
angelslistDF=pd.DataFrame()
angelslistDF["ticker"]=angelslist
mariners= angelslistDF.join(metslistDF)
#print(mariners)
falcons= mariners.sort_values(by= "totalvalue")
print(falcons)
####################################################################
raiderslist=[]
for p in falcons["ticker"]:
    raiderslist.append(p)
ramslist=[]
for n in falcons["totalvalue"]:
    ramslist.append(n)
raiderslistDF=pd.DataFrame()
raiderslistDF["ticker"]=raiderslist
ramslistDF=pd.DataFrame()
ramslistDF["totalvalue"]=ramslist
oakleylist=[]
for u in range(505):
    oakleylist.append(u)
#print(oakleylist)
oakleylistDF=pd.DataFrame()
oakleylistDF["rank"]=oakleylist
steelers= oakleylistDF.join(raiderslistDF).join(ramslistDF)
print(steelers)
steelers.to_sql("flumooscore", engine)