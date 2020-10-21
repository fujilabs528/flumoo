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

###############################################################
revenue_list = []
grossprofit_list = []
operatingincome_list = []
pretaxincome_list= []
netincome_list= []

brewers = 0
p = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT', 'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI', 'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP', 'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TDY', 'TFX', 'TXN', 'TXT', 'TMO', 'TIF', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VIAC', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']

for www in p:
    url = "https://finance.yahoo.com/quote/" + www + "/financials?p=" + www
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    my_table = soup.find("div", {"class": "D(tbrg)"})

    try:
        data = my_table.findAll("div", {"Ta(c) Py(6px) Bxz(bb) BdB Bdc($seperatorColor) Miw(120px) Miw(140px)--pnclg "
                                        "Bgc($lv1BgColor) fi-row:h_Bgc($hoverBgColor) D(tbc)"})
    except Exception as e:
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)

    xxx_list = []
    for xrays in data:
        xxx_list.append(xrays.text)

    try:
        revenue_list.append(xxx_list[0])
    except Exception as e:
        revenue_list.append("0")
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)
    try:
        grossprofit_list.append(xxx_list[6])
    except Exception as e:
        grossprofit_list.append("0")
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)
    try:
        operatingincome_list.append(xxx_list[12])
    except Exception as e :
        operatingincome_list.append("0")
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)
    try:
        pretaxincome_list.append(xxx_list[21])
    except Exception as e:
        pretaxincome_list.append("0")
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)
    try:
        netincome_list.append(xxx_list[27])
    except Exception as e:
        netincome_list.append("0")
        f = " |  富士 • F U J I   L A B S • 実験室  | "
        print(f, e)


    print(revenue_list)
    print(grossprofit_list)
    print(operatingincome_list)
    print(pretaxincome_list)
    print(netincome_list)

    print(brewers,www)
    brewers = brewers + 1

print(len(revenue_list))
print(len(grossprofit_list))
print(len(operatingincome_list))
print(len(pretaxincome_list))
print(len(netincome_list))


revenue_listDF = pd.DataFrame()
revenue_listDF["revenue"] = revenue_list

grossprofit_listDF = pd.DataFrame()
grossprofit_listDF["gross profit"] = grossprofit_list

operatingincome_listDF = pd.DataFrame()
operatingincome_listDF["operating income"] = operatingincome_list

pretaxincome_listDF = pd.DataFrame()
pretaxincome_listDF["pretax income"] = pretaxincome_list

netincome_listDF = pd.DataFrame()
netincome_listDF["net income"] = netincome_list

mariners = (revenue_listDF.join(grossprofit_listDF).join(operatingincome_listDF).
            join(pretaxincome_listDF).join(netincome_listDF))

print(mariners)

###############################################################

t = time.localtime()
auto_generating_file_title = time.strftime("%H:%M:%S", t)

engine = create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
mariners.to_sql(auto_generating_file_title, engine)



