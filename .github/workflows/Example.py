# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 23:05:36 2023

@author: sarth
"""

import requests
import json
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import datetime , timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor



url_oc      = "https://www.nseindia.com/option-chain"
url_nf      = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

sess = requests.Session()
cookies = dict()

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}

df_error = pd.DataFrame(columns=['StockSymbol'])


def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def get_data(url):
    try:
        
        set_cookie()
        response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
        # if(response.status_code==401):
        #     set_cookie()
        #     response = sess.get(url_nf, headers=headers, timeout=10, cookies=cookies)
        if(response.status_code==200):
            return response.text
    except:
        print('some error occured in function')    
    return ""

set_cookie()

data = ['RELIANCE']
inputStocksDataFrame = pd.read_csv('/home/runner/work/buysell/buysell/.github/workflows/StockLotSize.csv')
#pd.DataFrame(data , columns=['StockSymbol'])

inputStockList = inputStocksDataFrame['StockSymbol'].tolist()

df_combined_stock = pd.DataFrame()
df_error_list = []


for stockSymbol  in inputStocksDataFrame['StockSymbol']:
    print(stockSymbol)
    
    stockjsondata = get_data('https://www.nseindia.com/api/option-chain-equities?symbol='+stockSymbol)
    
    if(stockjsondata=='' or stockjsondata=={}):
        print('hi')
        df_error_list.append(stockSymbol)
    else:
        developerStock = json.loads(stockjsondata);
        #print(developerStock)
        recordsStock = developerStock['records']
        dataStock = recordsStock['data']
        df_read_stock = pd.json_normalize(dataStock)
        df_combined_stock = pd.concat([df_read_stock ,  df_combined_stock], ignore_index=True)

df_combined_stock = pd.DataFrame()
df_error_list = []


for stockSymbol  in inputStocksDataFrame['StockSymbol']:
    print(stockSymbol)
    
    stockjsondata = get_data('https://www.nseindia.com/api/option-chain-equities?symbol='+stockSymbol)
    
    if(stockjsondata=='' or stockjsondata=={}):
        
        df_error_list.append(stockSymbol)
    else:
        developerStock = json.loads(stockjsondata);
        #print(developerStock)
        recordsStock = developerStock['records']
        dataStock = recordsStock['data']
        df_read_stock = pd.json_normalize(dataStock)
        df_combined_stock = pd.concat([df_read_stock, df_combined_stock ], ignore_index=True)



for stockSymbol  in df_error['StockSymbol']:
    print(stockSymbol)

    stockjsondata = get_data('https://www.nseindia.com/api/option-chain-equities?symbol='+stockSymbol)
    #print(stockjsondata )
    if(stockjsondata=='' or stockjsondata=={}):
        print('hi')
    else:
        developerStock = json.loads(stockjsondata);
        recordsStock = developerStock['records']
        dataStock = recordsStock['data']
        df_read_stock = pd.json_normalize(dataStock)
        df_combined_stock = pd.concat([df_read_stock , df_combined_stock] , ignore_index=True)



niftyjsondata = get_data('https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20TOTAL%20MARKET')    
niftydata = json.loads(niftyjsondata)
niftyfetched = niftydata['data']
df_read_nifty = pd.json_normalize(niftyfetched)



df_combined_stock.rename(columns={'PE.bidprice':'PEbidprice'}, inplace=True)
df_combined_stock.rename(columns={'PE.underlying':'PEunderlying'}, inplace=True)
df_combined_stock.rename(columns={'PE.askPrice':'PEaskPrice'}, inplace=True)


df_combined_stock.rename(columns={'CE.bidprice':'CEbidprice'}, inplace=True)
df_combined_stock.rename(columns={'CE.askPrice':'CEaskPrice'}, inplace=True)
df_combined_stock.rename(columns={'CE.underlying':'CEunderlying'}, inplace=True)



columns_to_extract = ['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'yearHigh', 'yearLow', 'perChange30d']
df_extracted = df_read_nifty[columns_to_extract]
df_nifty_extracted = df_read_nifty[columns_to_extract]
print(df_extracted)

columns_to_extract_combined = ['CEunderlying', 'CEbidprice', 'CEaskPrice' , 'strikePrice' , 'expiryDate']
df_combined_stock_extracted = df_combined_stock[columns_to_extract_combined]
df_combined_stock_extracted = df_combined_stock_extracted.dropna(subset=['CEunderlying'])

filtered_df = df_combined_stock_extracted[~df_combined_stock_extracted['CEbidprice'].isin([0, 0.05])]
filtered_df.rename(columns={'CEunderlying': 'symbol'}, inplace=True)
inputStocksDataFrame.rename(columns={'StockSymbol': 'symbol'}, inplace=True)
# Reset the index after filtering rows
filtered_df = filtered_df.reset_index(drop=True)

pre_merged_call_df = pd.merge(filtered_df, df_nifty_extracted, on='symbol', how='inner')

merged_df = pd.merge(pre_merged_call_df, inputStocksDataFrame, on='symbol', how='inner')

# If you want to reset the index after merging
merged_df = merged_df.reset_index(drop=True)
merged_df['expiryDate'] = pd.to_datetime(merged_df['expiryDate'], format='%d-%b-%Y')
current_date = datetime.today()
merged_df['Date_Difference'] = (merged_df['expiryDate'] - current_date).dt.days
merged_df['GAP'] = ((merged_df['strikePrice'] - merged_df['lastPrice']) / merged_df['lastPrice']) * 100

merged_df = merged_df[merged_df['strikePrice'] >= merged_df['lastPrice']* 1.15]
merged_df = merged_df[merged_df['expiryDate'].dt.month == current_date.month]
merged_df['strikePrice'] = merged_df['strikePrice'].apply(lambda x: f'{x:.1f}' if x != int(x) else str(int(x)))





# Perform a self-join on the 'symbol' column
self_joined_df = pd.merge(merged_df, merged_df, on='symbol', how='inner', suffixes=('_left', '_right'))

# Filter rows where strikePrice_left is greater than strikePrice_right
filtered_self_joined_df = self_joined_df[self_joined_df.strikePrice_left < self_joined_df.strikePrice_right]
filtered_self_joined_df['Price_Difference'] = filtered_self_joined_df['CEbidprice_left'] - filtered_self_joined_df['CEaskPrice_right']
filtered_self_joined_df['Profit'] = (filtered_self_joined_df['LotSize_left'] * filtered_self_joined_df['Price_Difference'])-100
filtered_self_joined_df = filtered_self_joined_df[filtered_self_joined_df['Profit'] >= 260]

columns_to_drop = ['expiryDate_right', 'open_right', 'dayHigh_right', 'dayLow_right', 'lastPrice_right', 
                   'yearHigh_right', 'yearLow_right', 'perChange30d_right', 'LotSize_right', 
                   'Date_Difference_right', 'GAP_right' , 'CEaskPrice_left' , 'CEaskPrice_right']



filtered_self_joined_df = filtered_self_joined_df.drop(columns=columns_to_drop)

column_rename_dict = {
    'CEbidprice_left': 'Sell_Price',
    'strikePrice_left': 'Strike_Price_Sell',
    'CEbidprice_right': 'Buy_Price',
    'strikePrice_right': 'Strike_Price_Buy'
}

filtered_self_joined_df = filtered_self_joined_df.rename(columns=column_rename_dict)

# Reset the index
filtered_self_joined_df = filtered_self_joined_df.reset_index(drop=True)

filtered_self_joined_df.columns = [col.replace('_left', '') for col in filtered_self_joined_df.columns]


filtered_self_joined_df['expiryDate'] = pd.to_datetime(filtered_self_joined_df['expiryDate']).dt.strftime('%d-%b-%y')

print(filtered_self_joined_df.to_string())
# Display the filtered self-joined DataFrame
print(filtered_self_joined_df)


def drop_rows_by_count(group):
    count = len(group)
    if count > 60:
        return group.nlargest(count - 48, 'GAP', keep='last')
    elif 50 <= count <= 60:
        return group.nlargest(count - 32, 'GAP', keep='last')
    elif 45 <= count <= 50:
        return group.nlargest(count - 30, 'GAP', keep='last')
    elif 40 <= count < 45:
        return group.nlargest(count - 25, 'GAP', keep='last')
    elif 35 <= count < 40:
        return group.nlargest(count - 15, 'GAP', keep='last')
    elif 30 <= count < 35:
        return group.nlargest(count - 10, 'GAP', keep='last')
    elif 25 <= count < 30:
        return group.nlargest(count - 10, 'GAP', keep='last')
    elif 15 <= count < 20:
        return group.nlargest(count - 5, 'GAP', keep='last')
    else:
        return group
    
# filtered_self_joined_df = (
#     filtered_self_joined_df
#     .groupby('symbol', group_keys=False)
#     .apply(lambda group: group if len(group) <= 50 else group.nlargest(50, 'GAP'))
#     .reset_index(drop=True)
# )

filtered_self_joined_df = (
    filtered_self_joined_df
    .groupby('symbol', group_keys=False)
    .apply(lambda group: drop_rows_by_count(group))
    .reset_index(drop=True)
)




options = Options()
ua = UserAgent()
userAgent = ua.random

ua = UserAgent()
userAgent = ua.random



# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
chrome_options.add_argument("--window-size=1920x1080")

chrome_options.add_argument(f'user-agent={userAgent}')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument("--disable-blink-features")
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

combinedCallMarginList = []




filtered_self_joined_df.to_csv('C:\\Users\\sarth\\file1.csv')
def get_margin_values_call(params ):
        Strike  , StockName, BuyerPrice , GAP , LTP , Open , DayHigh , DayLow , FiftyWeekHigh , FiftyWeekLow , Expiry , ThirtyDay ,Days_Till_Expiry , Total_Profit , Buy_Price , Strike_Price_Buy = params
        driver = webdriver.Chrome(options=chrome_options)
        print(Strike)
        driver.get("https://zerodha.com/margin-calculator/SPAN/")   
        time.sleep(1)   
        driver.find_element(By.XPATH, '//*[@id="form-span"]/div/p[6]/label[3]/input').click()
        driver.find_element(By.XPATH, '//*[@id="product"]/option[2]').click()
        driver.find_element(By.XPATH, '//*[@id="strike_price"]').send_keys(Strike)
        driver.find_element(By.XPATH, '//*[@id="select2-scrip-container"]').click()
        time.sleep(0.5)
        driver.find_element(By.XPATH, '/html/body/span/span/span[1]/input').send_keys(StockName + ' ' + Expiry)
        time.sleep(0.5)
        driver.find_element(By.XPATH, '/html/body/span/span/span[2]/ul/li[1]').click()
        time.sleep(0.5)
        driver.find_element(By.XPATH, '//*[@id="form-span"]/div/p[7]/input[1]').click() 
        time.sleep(1.2)
        driver.find_element(By.XPATH, '//*[@id="strike_price"]').clear()
        driver.find_element(By.XPATH, '//*[@id="strike_price"]').send_keys(Strike_Price_Buy)####Change here
        driver.find_element(By.XPATH, '//*[@id="form-span"]/div/p[6]/label[2]/input').click()
        time.sleep(1)
        driver.find_element(By.XPATH, '//*[@id="form-span"]/div/p[7]/input[1]').click()
        time.sleep(1)
            
        margin = driver.find_element(By.XPATH, '//*[@id="tally"]/p[5]/span').text
        margin = margin.replace('Rs. ', '')
        margin = margin.replace(',', '')
        

        print(margin)
        print(Strike)
        print(StockName)
        if(margin==0 or Total_Profit==0 or Days_Till_Expiry==0 ):
            ProfitPer = 0
        else:
            
            ProfitPer = (float(Total_Profit)/float(margin)) * 100 * (365/float(Days_Till_Expiry)) 
        

        #print('i am here 3')  
        combinedCallMarginList.append([Strike  , StockName, BuyerPrice, GAP  , LTP , Open , DayHigh , DayLow , FiftyWeekHigh , FiftyWeekLow , Expiry , ThirtyDay ,Days_Till_Expiry , float(margin) , Total_Profit , ProfitPer, Buy_Price , Strike_Price_Buy]) 
  
        

scrape_params = [
    (
        row['Strike_Price_Sell'],
        row['symbol'],
        row['Sell_Price'],
        row['GAP'],
        row['lastPrice'],
        row['open'],
        row['dayHigh'],
        row['dayLow'],
        row['yearHigh'],
        row['yearLow'],
		row['expiryDate'],
        row['perChange30d'],
        row['Date_Difference'],
        row['Profit'],
        row['Buy_Price'],
		row['Strike_Price_Buy']
    )
    for _, row in filtered_self_joined_df.iterrows()
]



num_threads = 6  # You can adjust this based on your needs
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    executor.map(get_margin_values_call, scrape_params)
print('hi')

print(combinedCallMarginList)
combinedCallMarginFrame = pd.DataFrame(combinedCallMarginList,  columns =['Strike' 
                                ,'StockName'
                                ,'BuyerPrice' 
                                ,'GAP' 
                                ,'LTP' 
                                ,'Open'
                                ,'DayHigh'
                                ,'DayLow'
                                ,'50 Week High'
                                ,'50 Week Low'
                                ,'Expiry'
                                ,'30Day%'
                                ,'Days_Till_Expiry'
                                ,'margin'
                                ,'Total_Profit'
                                ,'Profit%'
								,'Buy_Price'
								,'Strike_Price_Buy'
                                ])



# for outer in range(0, len(filtered_self_joined_df)):    
#     try:
#         get_margin_values_call(filtered_self_joined_df.iloc[outer]['Strike_Price_Sell']  
#                           ,filtered_self_joined_df.iloc[outer]['symbol'] 
#                           ,filtered_self_joined_df.iloc[outer]['Sell_Price'] 
#                           ,filtered_self_joined_df.iloc[outer]['GAP'] 
#                           ,filtered_self_joined_df.iloc[outer]['lastPrice'] 
#                           ,filtered_self_joined_df.iloc[outer]['open'] 
#                           ,filtered_self_joined_df.iloc[outer]['dayHigh'] 
#                           ,filtered_self_joined_df.iloc[outer]['dayLow'] 
#                           ,filtered_self_joined_df.iloc[outer]['yearHigh'] 
#                           ,filtered_self_joined_df.iloc[outer]['yearLow'] 
#                           ,filtered_self_joined_df.iloc[outer]['expiryDate'] 
#                           ,filtered_self_joined_df.iloc[outer]['perChange30d'] 
#                           ,filtered_self_joined_df.iloc[outer]['Date_Difference'] 
#                           ,filtered_self_joined_df.iloc[outer]['Profit'] 
#                           ,filtered_self_joined_df.iloc[outer]['Buy_Price'] 
#                           ,filtered_self_joined_df.iloc[outer]['Strike_Price_Buy'] 
#                           );   
#     except ValueError as e:
#             print('divided by zero')  
#             print(e)
        



combinedCallMarginFrame = pd.DataFrame(combinedCallMarginList,  columns =['Strike' 
                                ,'StockName'
                                , 'BuyerPrice' 
                                , 'GAP' 
                                , 'LTP' 
                                , 'Open'
                                ,'DayHigh'
                                ,'DayLow'
                                ,'50 Week High'
                                ,'50 Week Low'
                                ,'Expiry'
                                ,'30Day%'
                                ,'Days_Till_Expiry'
                                ,'margin'
                                ,'Total_Profit'
                                ,'Profit%'
                                ,'Buy_Price'
                                ,'Strike_Price_Buy'
                                ])



print(combinedCallMarginFrame)




combinedCallMarginFrame = combinedCallMarginFrame.sort_values('Profit%', ascending=[False])

email = "callput.automate@gmail.com" # the email where you sent the email
password = 'pgcirdkstamiueeg'
send_to_email = ['callput.automate@gmail.com','sarthakgr@infinite.com' , 'gaurav.grover@jindalpower.com']  # for whom


msg = MIMEMultipart()
msg['Subject'] = "Call data  current month"



html = """\
<html>
  <head></head>
  <body>
<p>Hi!<br>
            Option Chain <br>
            {0}


        </p>
  </body>
</html>
""".format(combinedCallMarginFrame.to_html(index=False) )

part1 = MIMEText(html, 'html')
msg.attach(part1)

#.format(cdf.to_html(), df.to_html())


server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(email, password)
text = msg.as_string()
server.sendmail(email, send_to_email, text)
server.quit()

