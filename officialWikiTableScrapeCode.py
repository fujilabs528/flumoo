  import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

url= "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
page= requests.get(url)
soup= BeautifulSoup(page.content,"html.parser")
my_table = soup.find("table",{"class":"wikitable sortable"})
data= my_table.findAll("td")

listy= []
for xrays in data:
    listy.append(xrays.text)
listy=[item.strip() for item in listy]


ticker= (listy[0:5000:9])
tickerdf= pd.DataFrame()
tickerdf["ticker"]= ticker
#print(ticker)

name= (listy[1:5000:9])
namedf= pd.DataFrame()
namedf["names"]= name

reports= (listy[2:5000:9])
reportsdf=pd.DataFrame()
reportsdf["reports"]=reports

gics=(listy[3:5000:9])
gicsdf=pd.DataFrame()
gicsdf["gics"]=gics

gicssub=(listy[4:5000:9])
gicssubdf=pd.DataFrame()
gicssubdf["gicsub"]=gicssub

headquarters= (listy[5:5000:9])
headquartersdf=pd.DataFrame()
headquartersdf["hq"]=headquarters

sp500adddate= (listy[6:5000:9])
sp500adddatedf=pd.DataFrame()
sp500adddatedf["sp500adddate"]=sp500adddate

cik= (listy[7:5000:9])
cikdf=pd.DataFrame()
cikdf["cik"]=cik

founddate= (listy[8:5000:9])
founddatedf=pd.DataFrame()
founddatedf["founddate"]=founddate

mariners= (tickerdf.join(namedf).join(gicsdf).
            join(gicssubdf).join(headquartersdf).join(sp500adddatedf).join(cikdf).join(founddatedf))
print(mariners)

engine= create_engine('postgresql://postgres:password@localhost:5432/sp500xxx')
mariners.to_sql("wikiTableScrape",engine)

#engine= create_engine("sqlite://sp500.db")
#mariners.to_sql("company_page",engine)

#reds= pd.read_sql("wwweeerrrttt",engine)
#print(reds)
