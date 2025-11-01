import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt 
import sqlite3 

df = pd.read_csv("OnlineRetail.csv", encoding='latin1')
#print(df.head())
#print(df.isnull().sum())
df = df.dropna(subset=['CustomerID'])
#print(df.isnull().sum())
df.drop_duplicates(inplace=True)
df = df[df['Quantity']>0]
df['TotalAmount'] = df['Quantity']*df['UnitPrice']
#print(df.describe())
#print(df.info())
 # EDA
top_products = df.groupby('Description')['Quantity'].sum().sort_values(ascending=False).head(15)
bottom_products = df.groupby('Description')['Quantity'].sum().sort_values(ascending=True).head(15)
#print(top_products)
#print(bottom_products)
country_sales = df.groupby('Country')['TotalAmount'].sum().sort_values(ascending=False)
country_sales.head(10).plot(kind='bar', title='Top 10 Countries by Sales')
plt.show()

#recency,frequency,monetary analysis
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce', infer_datetime_format=True)
df = df.dropna(subset=['InvoiceDate'])
df = df.dropna(subset=['CustomerID'])
df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
latest_date = df['InvoiceDate'].max() + pd.Timedelta(days=1)

rfm = df.groupby('CustomerID').agg({
    'InvoiceDate': lambda x: int((np.datetime64(latest_date) - np.datetime64(x.max())) / np.timedelta64(1, 'D')),
    'InvoiceNo': 'count',
    'TotalAmount': 'sum'
})
    
rfm.rename(columns={'InvoiceDate' : 'Recency', 'InvoiceNo' : 'Frequency', 'TotalAmount' : 'Monetary'}, inplace=True)
rfm['R_Score'] = pd.qcut(rfm['Recency'], 4, labels=[4,3,2,1])
rfm['F_Score'] = pd.qcut(rfm['Frequency'], 4, labels=[1,2,3,4])
rfm['M_Score'] = pd.qcut(rfm['Monetary'], 4, labels=[1,2,3,4])

rfm['RFM_Segment'] = rfm[['R_Score','F_Score','M_Score']].sum(axis=1)
rfm['RFM_Level'] = pd.cut(rfm['RFM_Segment'], bins=[3,5,7,9,12], labels=['Low','Medium','High','Loyal'])

#print(rfm.head())

#connecting sql
conn = sqlite3.connect("ecommerce.db")
df.to_sql("EcommerceData", conn, if_exists='replace', index=False)
print("Data written successfully.")

#SQL QUERY
#monthly sales trend 
query = """
SELECT 
    strftime('%Y-%m', InvoiceDate) AS Month,
    ROUND(SUM(TotalAmount), 2) AS MonthlySales
FROM ECommerceData
GROUP BY Month
ORDER BY Month;
"""
monthly_sales = pd.read_sql_query(query, conn)
print(monthly_sales)

#top 10 products sold
query = """
SELECT Description, SUM(Quantity) AS UnitSold
FROM ECommerceData
GROUP BY Description
ORDER BY UnitSold DESC
LIMIT 10;
"""
top_products = pd.read_sql_query(query, conn)
print(top_products)

#Average order value according to country 
query = """
SELECT Country, 
       ROUND(SUM(TotalAmount) / COUNT(DISTINCT InvoiceNo), 2) AS AvgOrderValue
FROM ECommerceData
GROUP BY Country 
ORDER BY AvgOrderValue DESC;
"""
avg_order_val = pd.read_sql_query(query,conn)
print(avg_order_val)
conn = sqlite3.connect("ecommerce.db")
df = pd.read_sql_query("SELECT * FROM ECommerceData", conn)
df.to_csv("ecommerce_cleaned.csv", index=False)
conn.close()
#df.to_csv("ecommerce_cleaned.csv", index=False)
