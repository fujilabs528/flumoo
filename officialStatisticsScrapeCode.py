import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2

###############################################################

p = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT', 'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI', 'HIG', 'HAS', 'HCA', 'PEAK', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP', 'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TDY', 'TFX', 'TXN', 'TXT', 'TMO', 'TIF', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VIAC', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']


def baseball(p):
    yankees= []
    brewers= 0
    for x in p:
        url = "https://finance.yahoo.com/quote/" + x + "/key-statistics?p=" + x
        time.sleep(2)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        my_table = soup.find("table", {"class": "W(100%) Bdcl(c) M(0) Whs(n) BdEnd Bdc($seperatorColor) D(itb)"})

        try:
            data = my_table.findAll("td", {"Ta(c) Pstart(10px) Miw(60px) Miw(80px)--pnclg Bgc($lv1BgColor) fi-row:h_Bgc($hoverBgColor)"})
            for xrays in data:
                yankees.append(xrays.text)
        except Exception as e:
            f= " | F U J I - L A B S • 富士研究所 | "
            print(f,e)

        #try:
        #    data1 = my_table.findAll("td", {"Ta(c) Pstart(10px) Miw(60px) Miw(80px)--pnclg fi-row:h_Bgc($hoverBgColor) Bgc($lv1BgColor)"})
        #    for twins in data1:
        #        yankees.append(twins.text)
        #except Exception as e:
        #    print(" *** *** | F U J I - L A B S | *** ***")
        #    print(e)

        #try:
        #    data2 = my_table.findAll("td", {"Ta(c) Pstart(10px) Miw(60px) Miw(80px)--pnclg fi-row:h_Bgc($hoverBgColor) Pend(10px) Bgc($lv1BgColor)"})
        #    for reds in data2:
        #        yankees.append(reds.text)
        #except Exception as e:
        #    print(" *** *** | F U J I - L A B S | *** ***")
        #    print(e)

        print(brewers)
        print(x)
        brewers= brewers + 1
    #print(yankees)

###############################################################

    currentMarketCap = (yankees[0:10000:9])
    currentMarketCapDF = pd.DataFrame()
    currentMarketCapDF["currentMarketCap"] = currentMarketCap

    currentEnterpriseValue = (yankees[1:10000:9])
    currentEnterpriseValueDF = pd.DataFrame()
    currentEnterpriseValueDF["currentEnterpriseValue"] = currentEnterpriseValue

    currentTrailingPE = (yankees[2:10000:9])
    currentTrailingPEDF = pd.DataFrame()
    currentTrailingPEDF["currentTrailingPE"] = currentTrailingPE

    currentForwardPE = (yankees[3:10000:9])
    currentForwardPEDF = pd.DataFrame()
    currentForwardPEDF["currentForwardPE"] = currentForwardPE

    currentPEGRatio = (yankees[4:10000:9])
    currentPEGRatioDF = pd.DataFrame()
    currentPEGRatioDF["currentPEGRatio"] = currentPEGRatio

    currentPriceToSales = (yankees[5:10000:9])
    currentPriceToSalesDF = pd.DataFrame()
    currentPriceToSalesDF["currentPriceToSales"] = currentPriceToSales

    currentPriceToBook = (yankees[6:10000:9])
    currentPriceToBookDF = pd.DataFrame()
    currentPriceToBookDF["currentPriceToBook"] = currentPriceToBook

    currentEnterpriseValueToRev = (yankees[7:10000:9])
    currentEnterpriseValueToRevDF = pd.DataFrame()
    currentEnterpriseValueToRevDF["currentEnterpriseValueToRev"] = currentEnterpriseValueToRev

    currentEnterpirseValueToEBITDA = (yankees[8:10000:9])
    currentEnterpirseValueToEBITDADF = pd.DataFrame()
    currentEnterpirseValueToEBITDADF["currentEnterpirseValueToEBITDA"] = currentEnterpirseValueToEBITDA

###############################################################

    MarchMarketCap = (yankees[9:10000:27])
    MarchMarketCapDF = pd.DataFrame()
    MarchMarketCapDF["MarchMarketCap"] = MarchMarketCap

    MarchEnterpriseValue = (yankees[10:10000:27])
    MarchEnterpriseValueDF = pd.DataFrame()
    MarchEnterpriseValueDF["MarchEnterpriseValue"] = MarchEnterpriseValue

    MarchTrailingPE = (yankees[11:10000:27])
    MarchTrailingPEDF = pd.DataFrame()
    MarchTrailingPEDF["MarchTrailingPE"] = MarchTrailingPE

    MarchForwardPE = (yankees[12:10000:27])
    MarchForwardPEDF = pd.DataFrame()
    MarchForwardPEDF["MarchForwardPE"] = MarchForwardPE

    MarchPEGRatio = (yankees[13:10000:27])
    MarchPEGRatioDF = pd.DataFrame()
    MarchPEGRatioDF["MarchPEGRatio"] = MarchPEGRatio

    MarchPriceToSales = (yankees[14:10000:27])
    MarchPriceToSalesDF = pd.DataFrame()
    MarchPriceToSalesDF["MarchPriceToSales"] = MarchPriceToSales

    MarchPriceToBook = (yankees[15:10000:27])
    MarchPriceToBookDF = pd.DataFrame()
    MarchPriceToBookDF["MarchPriceToBook"] = MarchPriceToBook

    MarchEnterpriseValueToRev = (yankees[16:10000:27])
    MarchEnterpriseValueToRevDF = pd.DataFrame()
    MarchEnterpriseValueToRevDF["MarchEnterpriseValueToRev"] = MarchEnterpriseValueToRev

###############################################################

    SepMarketCap = (yankees[17:10000:27])
    SepMarketCapDF = pd.DataFrame()
    SepMarketCapDF["SepMarketCap"] = SepMarketCap

    SepEnterpriseValue = (yankees[18:10000:27])
    SepEnterpriseValueDF = pd.DataFrame()
    SepEnterpriseValueDF["SepEnterpriseValue"] = SepEnterpriseValue

    SepTrailingPE = (yankees[19:10000:27])
    SepTrailingPEDF = pd.DataFrame()
    SepTrailingPEDF["SepTrailingPE"] = SepTrailingPE

    SepForwardPE = (yankees[20:10000:27])
    SepForwardPEDF = pd.DataFrame()
    SepForwardPEDF["SepForwardPE"] = SepForwardPE

    SepPEGRatio = (yankees[21:10000:27])
    SepPEGRatioDF = pd.DataFrame()
    SepPEGRatioDF["SepPEGRatio"] = SepPEGRatio

    SepPriceToSales = (yankees[22:10000:27])
    SepPriceToSalesDF = pd.DataFrame()
    SepPriceToSalesDF["SepPriceToSales"] = SepPriceToSales

    SepPriceToBook = (yankees[23:10000:27])
    SepPriceToBookDF = pd.DataFrame()
    SepPriceToBookDF["SepPriceToBook"] = SepPriceToBook

    SepEnterpriseValueToRev = (yankees[24:10000:27])
    SepEnterpriseValueToRevDF = pd.DataFrame()
    SepEnterpriseValueToRevDF["SepEnterpriseValueToRev"] = SepEnterpriseValueToRev

    SepEnterpirseValueToEBITDA = (yankees[25:10000:27])
    SepEnterpirseValueToEBITDADF = pd.DataFrame()
    SepEnterpirseValueToEBITDADF["SepEnterpirseValueToEBITDA"] = SepEnterpirseValueToEBITDA

###############################################################

    mariners = (currentMarketCapDF.join(currentEnterpriseValueDF).join(currentTrailingPEDF).join(currentForwardPEDF).
                join(currentPEGRatioDF).join(currentPriceToSalesDF).join(currentPriceToBookDF).
                join(currentEnterpriseValueToRevDF).join(currentEnterpirseValueToEBITDADF))

                #join(MarchMarketCapDF).join(MarchEnterpriseValueDF).join(MarchTrailingPEDF).join(MarchForwardPEDF).
                #join(MarchPEGRatioDF).join(MarchPriceToSalesDF).join(MarchPriceToBookDF).
                #join(MarchEnterpriseValueToRevDF).join(MarchEnterpriseValueToRevDF).

                #join(SepMarketCapDF).join(SepEnterpriseValueDF).join(SepTrailingPEDF).join(SepForwardPEDF).
                #join(SepPEGRatioDF).join(SepPriceToSalesDF).join(SepPriceToBookDF).
                #join(SepEnterpriseValueToRevDF).join(SepEnterpirseValueToEBITDADF))

    print(mariners)

    engine= create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
    mariners.to_sql("statisticsScrape",engine)



baseball(p)
