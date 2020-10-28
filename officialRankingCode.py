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
variable= ["Alpha", "Bravo", "Charlie", "Delta" , "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet", "Kilo", "Lima", "Mike",
           "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango", "Uniform", "Victor", "Whiskey", "X-ray", "Yankee", "Zulu"]

engine = create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
alpha= pd.read_sql_table("incomeStatementScrape",engine)

###########################################################
# CONVERT OBJECT TO FLOAT64
#print(alpha.dtypes)

newlist = []
oldlist= []
extralist= []
overlist= []
boobylist= []
def bigpussy(xxx):
    p = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK',
         'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP',
         'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA',
         'AIV', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY',
         'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP',
         'BSX', 'BMY', 'AVGO', 'BR', 'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT',
         'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI',
         'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG',
         'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT', 'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI',
         'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG',
         'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX',
         'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD',
         'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS',
         'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM',
         'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI', 'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY',
         'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IEX',
         'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ',
         'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'KEYS',
         'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN',
         'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC',
         'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT',
         'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP', 'NFLX',
         'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG',
         'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC',
         'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG',
         'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O',
         'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB',
         'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE',
         'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TDY', 'TFX', 'TXN',
         'TXT', 'TMO', 'TIF', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB',
         'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ',
         'VRTX', 'VIAC', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST',
         'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH',
         'ZION', 'ZTS']

    tt = 0

    for x in alpha[xxx]:
        y= x.replace(",","")
        newlist.append(y)
        overlist.append(p[tt])
        tt=tt+1
    for q in newlist:
        p= q.replace("N/A","0")
        oldlist.append(p)
    #print(oldlist)

    for i in oldlist:
        h = i.replace("-","0")
        boobylist.append(h)

    for n in boobylist:
        n=float(n)
        extralist.append(n)

    #print(extralist)
    #print(overlist)

    extralistDF = pd.DataFrame()
    extralistDF[xxx]= extralist

    overlistDF= pd.DataFrame()
    overlistDF["ticker name"]= overlist

    mariners = (overlistDF.join(extralistDF))

    #print(mariners)


    mariners[xxx] = pd.to_numeric(mariners[xxx],errors="coerce")


    falcons= mariners.sort_values(by= xxx)

    #print(falcons)

    beaconlist = []
    for qqq in falcons["ticker name"]:
        beaconlist.append(qqq)

    crimsonlist = [ ]
    for www in falcons[xxx]:
        crimsonlist.append((www))


    lovelist = []
    ss=0
    for uuu in falcons[xxx]:
        lovelist.append(ss)
        ss = ss + 1
    #print(lovelist)

    beaconlistDF = pd.DataFrame()
    beaconlistDF["tickername"] = beaconlist

    crimsonlistDF = pd.DataFrame()
    crimsonlistDF[xxx] = crimsonlist

    lovelistDF = pd.DataFrame()
    lovelistDF["ranking"] = lovelist

    eagles = (lovelistDF.join(beaconlistDF).join(crimsonlistDF))

    #print(eagles)

    ##############################################################
    charlie = falcons.max()
    delta= falcons.min()
    echo= falcons.mean()
    foxtrot= falcons.median()
    golf=falcons.std()
    hotel= falcons.var()
    india=falcons.skew()
    juliet=falcons.kurtosis()


    print(xxx)
    print("MARKET REPORT - OCTOBER 21/2020")
    print("STATISTICS")
    print("MAX", charlie[xxx])
    print("MIN", delta[xxx])
    print("MEAN", echo[xxx])
    print("MEDIAN",foxtrot[xxx])
    print("STAN DEV",golf[xxx])
    print("VAR", hotel[xxx])
    print("SKEW",india[xxx])
    print("KURTOSIS",juliet[xxx])

    # RANGE INFO
    kilo= charlie[xxx]
    kilo= float(kilo)
    lima=delta[xxx]
    lima=float(lima)
    mike= (kilo-lima)
    print("RANGE", mike)
    november= mike/30
    #print("ITERATIONS OF 30")
    #print(november)
    ################################################################################

    # GRAPH INDEX - X AXIS
    index = []
    count= lima
    for x in range(30):
        index.append(count)
        count=count+november
    #print("INDEX")
    #print(index)
    ################################################################################

    # GRAPH VALUES - Y AXIS
    list_1=[]
    list_2=[]
    list_3=[]
    list_4=[]
    list_5=[]
    list_6=[]
    list_7=[]
    list_8=[]
    list_9=[]
    list_10=[]
    list_11=[]
    list_12=[]
    list_13=[]
    list_14=[]
    list_15=[]
    list_16=[]
    list_17=[]
    list_18=[]
    list_19=[]
    list_20=[]
    list_21=[]
    list_22=[]
    list_23=[]
    list_24=[]
    list_25=[]
    list_26=[]
    list_27=[]
    list_28=[]
    list_29=[]
    list_30=[]

    for l in falcons[xxx]:
        if l < index[1]:
            list_1.append(l)
        elif l < index[2] and l < index[1]:
            list_2.append(l)
        elif l < index[3] and l < index[2]:
            list_3.append(l)
        elif l < index[4] and l < index[3]:
            list_4.append(l)
        elif l < index[5] and l < index[4]:
            list_5.append(l)
        elif l < index[6] and l < index[5]:
            list_6.append(l)
        elif l < index[7] and l < index[6]:
            list_7.append(l)
        elif l < index[8] and l < index[7]:
            list_8.append(l)
        elif l < index[9] and l < index[8]:
            list_9.append(l)
        elif l < index[10] and l < index[9]:
            list_10.append(l)
        elif l < index[11] and l < index[10]:
            list_11.append(l)
        elif l < index[12] and l < index[11]:
            list_12.append(l)
        elif l < index[13] and l < index[12]:
            list_13.append(l)
        elif l < index[14] and l < index[13]:
            list_14.append(l)
        elif l < index[15] and l < index[14]:
            list_15.append(l)
        elif l < index[16] and l < index[15]:
            list_16.append(l)
        elif l < index[17] and l < index[16]:
            list_17.append(l)
        elif l < index[18] and l < index[17]:
            list_18.append(l)
        elif l < index[19] and l < index[18]:
            list_19.append(l)
        elif l < index[20] and l < index[19]:
            list_20.append(l)
        elif l < index[21] and l < index[20]:
            list_21.append(l)
        elif l < index[22] and l < index[21]:
            list_22.append(l)
        elif l < index[23] and l < index[22]:
            list_23.append(l)
        elif l < index[24] and l < index[23]:
            list_24.append(l)
        elif l < index[25] and l < index[24]:
            list_25.append(l)
        elif l < index[26] and l < index[25]:
            list_26.append(l)
        elif l < index[27] and l < index[26]:
            list_27.append(l)
        elif l < index[28] and l < index[27]:
            list_28.append(l)
        elif l < index[29] and l < index[28]:
            list_29.append(l)

    values_list = [len(list_1), len(list_2), len(list_3), len(list_4), len(list_5), len(list_6), len(list_7), len(list_8), len(list_9),
                    len(list_10), len(list_11), len(list_12), len(list_13), len(list_14), len(list_15),len(list_16), len(list_17), len(list_18),
                    len(list_19), len(list_20), len(list_21), len(list_22), len(list_23), len(list_24), len(list_25), len(list_26), len(list_27),
                    len(list_28), len(list_29), len(list_30)]

    #print(index)
    #print(values_list)

    print("FULL LIST")
    print(eagles)

    print("NEGATIVE OUTLIERS")
    print(falcons[0:20])

    print("POSITIVE OUTLIERS")
    print(falcons[485:505])

    t = time.localtime()
    auto_generating_file_title = time.strftime("%H:%M:%S", t)

    engine = create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
    eagles.to_sql(xxx, engine)

    #THIS CODE DOES NOT WORK _ COME BACK _ 10192020
    #tony= plt.bar(index,values_list, width=20, align="edge")
    #plt.xlabel(xxx)
    #plt.ylabel("count")
    #plt.show()

bigpussy("net income")