import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import time
###############################################################

p = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT', 'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI', 'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP', 'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TDY', 'TFX', 'TXN', 'TXT', 'TMO', 'TIF', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VIAC', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']
#print(len(p))

#j = 0
def bigtitties(p):
    yankees= []
    brewers= 0
    for x in p:
        #print(j)
        #print(p[j])
        url = "https://finance.yahoo.com/quote/" + x + "?p=" + x
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        my_table = soup.find("table", {"class": "W(100%)"})
        data = my_table.findAll("td", {"Ta(end) Fw(600) Lh(14px)"})
        my_table2 = soup.find("table", {"class": "W(100%) M(0) Bdcl(c)"})
        try:
            data2 = my_table2.findAll("td", {"Ta(end) Fw(600) Lh(14px)"})
        except Exception:
            print("*** | FUJI LABS | GENERAL EXCEPTION MESSAGE | ***")

        #yankees.append(data)
        #print(data)
        #listy = []
        for xrays in data:
            broncos= xrays.text
            rams = broncos.replace(",", "")
            chargers= rams.replace("B","")
            yankees.append(chargers)
            listy = [item.strip() for item in yankees]
        for twins in data2:
            titans= twins.text
            seahawks= titans.replace(",","")
            raiders= seahawks.replace("B","")
            yankees.append(raiders)
            listy = [item.strip() for item in yankees]
        #print(listy)
        print(brewers)
        print(x)
        brewers= brewers + 1
    #print(yankees)
    #return data

########################################################

    previousClose = (yankees[0:10000:16])
    previousCloseDF = pd.DataFrame()
    previousCloseDF["previousClose"] = previousClose

    open = (yankees[1:10000:16])
    openDF = pd.DataFrame()
    openDF["open"] = open

    bid = (yankees[2:10000:16])
    bidDF = pd.DataFrame()
    bidDF["big"] = bid

    ask = (yankees[3:10000:16])
    askDF = pd.DataFrame()
    askDF["ask"] = ask

    daysRange = (yankees[4:10000:16])
    daysRangeDF = pd.DataFrame()
    daysRangeDF["daysRange"] = daysRange

    fiftyTwoWeekRange = (yankees[5:10000:16])
    fiftyTwoWeekRangeDF = pd.DataFrame()
    fiftyTwoWeekRangeDF["fiftyTwoWeekRange"] = fiftyTwoWeekRange

    volume = (yankees[6:10000:16])
    volumeDF = pd.DataFrame()
    volumeDF["volume"] = volume

    averageVolume = (yankees[7:10000:16])
    averageVolumeDF = pd.DataFrame()
    averageVolumeDF["averageVolume"] = averageVolume

    marketCap = (yankees[8:10000:16])
    marketCapDF = pd.DataFrame()
    marketCapDF["marketCap"] = marketCap

    beta = (yankees[9:10000:16])
    betaDF = pd.DataFrame()
    betaDF["beta"] = beta

    peRatio = (yankees[10:10000:16])
    peRatioDF = pd.DataFrame()
    peRatioDF["peRatio"] = peRatio

    eps = (yankees[11:10000:16])
    epsDF = pd.DataFrame()
    epsDF["eps"] = eps

    earningsDate = (yankees[12:10000:16])
    earningsDateDF = pd.DataFrame()
    earningsDateDF["earningsDate"] = earningsDate

    forwardDividendPlusYield = (yankees[13:10000:16])
    forwardDividendPlusYieldDF = pd.DataFrame()
    forwardDividendPlusYieldDF["forwardDividendPlusYield"] = forwardDividendPlusYield

    exDividendDate = (yankees[14:10000:16])
    exDividendDateDF = pd.DataFrame()
    exDividendDateDF["exDividendDate"] = exDividendDate

    oneYearTargetEstimate = (yankees[15:10000:16])
    oneYearTargetEstimateDF = pd.DataFrame()
    oneYearTargetEstimateDF["oneYearTargetEstimate"] = oneYearTargetEstimate

    mariners = (previousCloseDF.join(openDF).join(bidDF).
                join(askDF).join(daysRangeDF).join(fiftyTwoWeekRangeDF).join(volumeDF).
                join(averageVolumeDF).join(marketCapDF).join(betaDF).join(peRatioDF).
                join(epsDF).join(earningsDateDF).join(forwardDividendPlusYieldDF).
                join(exDividendDateDF).join(oneYearTargetEstimateDF))

    print(mariners)

    t = time.localtime()
    auto_generating_file_title = time.strftime("%H:%M:%S", t)

    engine= create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
    mariners.to_sql(auto_generating_file_title,engine)


bigtitties(p)




