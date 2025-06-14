import sqlite3
import pandas as pd
import os
import logging
from ingestion_db import ingest_db
import time

start = time.time()

logging.basicConfig(
    filename= "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

def create_vendor_summary(conn):
    """this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data"""
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT
            VendorNumber,
            SUM(Freight) as TotalFreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseDollars
            FROM purchases p
                JOIN purchase_prices pp
                ON p.Brand = PP.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber,p.VendorName,p.Brand
            ORDER BY TotalPurchaseDollars
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(ExciseTax) as TotalExciseTax
            FROM Sales
            GROUP BY VendorNo, Brand
            ORDER BY TotalSalesDollars
    )
    SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.ActualPrice,
            ps.PurchasePrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.TotalFreightCost
            FROM PurchaseSummary ps
            LEFT JOIN SalesSummary ss 
                on ps.VendorNumber = ss.VendorNo
                AND ps.Brand = ss.Brand
            LEFT JOIN FreightSummary fs
                on ps.VendorNumber = fs.VendorNumber
            ORDER BY ps.TotalPurchaseDollars DESC""",conn)
    return vendor_sales_summary

def clean_data(df):
    """this function will clean the data"""
    # changing datatype to float
    df['Volume']=df['Volume'].astype('float64')

    # filling missing value with 0
    df.fillna(0, inplace = True)

    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # creating new colums for better analysis
    df['GrossProfit']=df['TotalSalesDollars']-df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']

    return df
    
if __name__ == '__main__':
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.......')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    
    logging.info('Cleaning Data.......')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting Data.......')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')   

end = time.time()
total_time = (end-start)/60
print(f'Total time taken:{total_time}')