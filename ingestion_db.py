import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename= "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine('sqlite:///inventory.db')

# creating a function to ingest data into a database
def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine , if_exists = 'append', index = False,chunksize = 10000) # chunksize is used to ingest data in chunks to solve memory error
# ingesting db into the database
def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine) # string slicing used to remove .csv  and use the same file name as table name
    end = time.time()
    total_time = (end-start)/60
    logging.info('-----------Ingestion Complete-------------')
    logging.info(f'\nTotal Time Taken : {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()