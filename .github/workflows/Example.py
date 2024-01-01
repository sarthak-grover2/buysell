
"""
Created on Tue Aug 29 10:12:50 2023

@author: sarth
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 23:05:36 2023

@author: sarth
"""

import requests
import json
import pandas as pd
import time
from datetime import datetime , timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from requests import Session


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
#inputStocksDataFrame = pd.read_csv('C:\\Users\\sarth\\Documents\\StockLotSize.csv')
#pd.DataFrame(data , columns=['StockSymbol'])

inputStockList = inputStocksDataFrame['StockSymbol'].tolist()

df_combined_stock = pd.DataFrame()
df_error_list = []


for stockSymbol  in inputStocksDataFrame['StockSymbol']:
    print(stockSymbol)
    try:
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
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {stockSymbol}: {e}")
        df_error_list.append(stockSymbol)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for {stockSymbol}: {e}")
        df_error_list.append(stockSymbol)

print('erorr frame processing start')
print(df_error)
print('erorr frame processing end')
for stockSymbol  in df_error['StockSymbol']: 
    try:
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
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {stockSymbol}: {e}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for {stockSymbol}: {e}")



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
filtered_df = df_combined_stock_extracted[~df_combined_stock_extracted['CEaskPrice'].isin([0, 0.05])]

current_month = datetime.today().month
filtered_df['expiryDate'] = pd.to_datetime(filtered_df['expiryDate'])

#filtered_df = filtered_df[filtered_df['expiryDate'].dt.month == current_month]

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
merged_df['Date_Difference'] = (merged_df['expiryDate'] - current_date).dt.days + 1
merged_df['GAP'] = ((merged_df['strikePrice'] - merged_df['lastPrice']) / merged_df['lastPrice']) * 100

merged_df = merged_df[merged_df['strikePrice'] >= merged_df['lastPrice']* 1.10]

merged_df['strikePrice'] = merged_df['strikePrice'].apply(lambda x: f'{x:.1f}' if x != int(x) else str(int(x)))


next_month = current_date.replace(day=1) + timedelta(days=32
                                                     )
next_month = next_month.replace(day=1)

current_date = datetime.today()
merged_df['Days_Till_Expiry'] = (merged_df['expiryDate'] - current_date).dt.days + 1

merged_df = merged_df[
    (merged_df['expiryDate'].dt.month == current_date.month) | ((merged_df['Date_Difference'] >= 8) & (merged_df['expiryDate'].dt.month == next_month.month))
]


# Perform a self-join on the 'symbol' column
self_joined_df = pd.merge(merged_df, merged_df, on=['symbol' , 'expiryDate' ], how='inner', suffixes=('_left', '_right'))

# Filter rows where strikePrice_left is greater than strikePrice_right
filtered_self_joined_df = self_joined_df[self_joined_df.strikePrice_left < self_joined_df.strikePrice_right]
filtered_self_joined_df['Price_Difference'] = filtered_self_joined_df['CEbidprice_left'] - filtered_self_joined_df['CEaskPrice_right']
filtered_self_joined_df['Profit'] = (filtered_self_joined_df['LotSize_left'] * filtered_self_joined_df['Price_Difference'])-100
filtered_self_joined_df = filtered_self_joined_df[filtered_self_joined_df['Profit'] >= 200]





filtered_self_joined_df.to_csv('C:\\Users\\sarth\\file1.csv')




















columns_to_drop = [ 'open_right', 'dayHigh_right', 'dayLow_right', 'lastPrice_right', 
                   'yearHigh_right', 'yearLow_right', 'perChange30d_right', 'LotSize_right', 
                   'Date_Difference_right', 'GAP_right' , 'CEaskPrice_left' ]



filtered_self_joined_df = filtered_self_joined_df.drop(columns=columns_to_drop)

column_rename_dict = {
    'CEbidprice_left': 'Sell_Price',
    'strikePrice_left': 'Strike_Price_Sell',
    'CEaskPrice_right': 'Buy_Price',
    'strikePrice_right': 'Strike_Price_Buy'
}

filtered_self_joined_df = filtered_self_joined_df.rename(columns=column_rename_dict)

# Reset the index
filtered_self_joined_df = filtered_self_joined_df.reset_index(drop=True)

filtered_self_joined_df.columns = [col.replace('_left', '') for col in filtered_self_joined_df.columns]


filtered_self_joined_df['expiryDate'] = pd.to_datetime(filtered_self_joined_df['expiryDate']).dt.strftime('%d-%b-%y')

filtered_self_joined_df['expiryDate'] = pd.to_datetime(filtered_self_joined_df['expiryDate'])
filtered_self_joined_df['scripDate'] = filtered_self_joined_df['symbol'] + filtered_self_joined_df['expiryDate'].dt.strftime('%y') + filtered_self_joined_df['expiryDate'].dt.strftime('%b') 
#print(filtered_self_joined_df.to_string())
# Display the filtered self-joined DataFrame
#print(filtered_self_joined_df)


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


print(filtered_self_joined_df.to_string())


filtered_self_joined_df.to_csv('C:\\Users\\sarth\\file1.csv')

json_template = {
    'action': 'calculate',
    'exchange[]': ['NFO' , 'NFO'],
    'product[]': ['OPT' , 'OPT'],
    'scrip[]': '',
    'option_type[]': ['CE' , 'CE'],
    'trade[]': ['sell' , 'buy']
}

# Create an empty list to store individual JSON records
json_records = []


payload = {
  "action": "calculate",
  "exchange[]": [
    "NFO",
    "NFO"
  ],
  "option_type[]": [
    "CE",
    "CE"
  ],
  "product[]": [
    "OPT",
    "OPT"
  ],
  "qty[]": [
    "1500",
    "1500"
  ],
  "scrip[]": [
    "SBIN23AUG",
    "SBIN23AUG"
  ],
  "strike_price[]": [
    "650",
    "660"
  ],
  "trade[]": [
    "sell",
    "buy"
  ]
}



BASE_URL = 'https://zerodha.com/margin-calculator/SPAN'

# Iterate over each row in the merged_df DataFrame
for index, row in filtered_self_joined_df.iterrows():
    json_record = json_template.copy()  # Create a copy of the template
    json_record['scrip[]'] = [row['scripDate'].upper(), row['scripDate'].upper()]  # Populate the 'scrip[]' field
    json_record['qty[]'] = [row['LotSize'], row['LotSize']]  # Populate the 'scrip[]' field
    json_record['strike_price[]'] = [row['Strike_Price_Sell'], row['Strike_Price_Buy']] 
    print(json_record)
    #json_records.append(json_record)
    
    print('hi')
    #print(json.dumps(json_record, indent=4))
    #execute = json.dumps(json_record, indent=4)
    #print(json_record)
    session = Session()
    try:
        res = session.post(BASE_URL, data=json_record)
        response_data = res.json()
        
        # Extract values under "total" key
        total_values = response_data.get('total', {})
        
        # Handle cases where the values are null, not available, or zero
        if total_values is None or len(response_data['last']) == 0 or len(response_data['total']) == 0:
            print('hello')
        else:
            
        

            overall_total = total_values.get('total', 0)
            
            if overall_total is None:
                print('hello')
            else:
                
                filtered_self_joined_df.loc[index, 'Margin'] = overall_total
        

        # Update the DataFrame or perform any other necessary actions with the extracted values
        
    except json.JSONDecodeError:
        print("Response content is not valid JSON")



        
#print(merged_df.to_string())
# Convert the list of JSON records to a JSON string

filtered_self_joined_df.to_csv('C:\\Users\\sarth\\file1.csv')
filtered_self_joined_df['Profit%'] = (filtered_self_joined_df['Profit'] / filtered_self_joined_df['Margin']) * 100 * (365 / filtered_self_joined_df['Days_Till_Expiry'])

#merged_df['Profit%'] = (float(merged_df['Profit'])/float(merged_df['Margin'])) * 100 * (365/float(merged_df['Days_Till_Expiry'])) 




filtered_self_joined_df = filtered_self_joined_df.sort_values('Profit%', ascending=[False])
filtered_self_joined_df = filtered_self_joined_df.drop(columns=['scripDate' , 'Date_Difference'])
filtered_self_joined_df = filtered_self_joined_df[filtered_self_joined_df['Profit%'] >= 10]




new_column_order = [
    'symbol', 'Profit%', 'GAP', 'lastPrice', 'Strike_Price_Sell',
    'Strike_Price_Buy', 'Price_Difference', 'Sell_Price', 'Buy_Price',
    'expiryDate', 'open', 'dayHigh', 'dayLow', 'yearHigh', 'yearLow',
    'perChange30d', 'LotSize', 'Days_Till_Expiry', 'Profit', 'Margin'
]

filtered_self_joined_df = filtered_self_joined_df[new_column_order]

email = "callput.automate@gmail.com" # the email where you sent the email
password = 'pgcirdkstamiueeg'
send_to_email = ['callput.automate@gmail.com','sarthakgr@infinite.com','gaurav.grover@jindalpower.com'
                 ] # for whom


msg = MIMEMultipart()
msg['Subject'] = "BUY SELL CALL Data Current Month"

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
""".format(filtered_self_joined_df.to_html(index=False) )

part1 = MIMEText(html, 'html')
msg.attach(part1)

#.format(cdf.to_html(), df.to_html())

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(email, password)
text = msg.as_string()
server.sendmail(email, send_to_email, text)
server.quit()

    
