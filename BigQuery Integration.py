# -*- coding: utf-8 -*-
"""
Created on Sat May 28 17:28:54 2022

@author: GRACE ESTRADA
"""
import time
from timeit import default_timer as timer
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

start = timer()

# %% CONNECTING TO BIGQUERY

print('\nConnecting to BigQuery . . .')
time.sleep(2)

credentials = service_account.Credentials.from_service_account_file('Config/brazil-real-estate-351608-faf83ded68f9.json')

project_id = 'brazil-real-estate-351608'

client = bigquery.Client(credentials = credentials, project = project_id)

print('Connected!')
time.sleep(2)


# %% DEFINE FUNCTION TO QUERY FROM SPECIFIC TABLES

def query_table(listing_type):
    
    """Function to query from specific listing type (rent or sell) table"""
    
    print(f'\nQuerying listings from {listing_type} tables . . .')
    time.sleep(2)

    #GENERATE THE TABLE LIST TO APPEND AS TABLE ID IN THE DATAFRAME

    tables = client.query("""
                         SELECT table_id FROM `properati-data-public.properties_br.__TABLES__`
                         """).result().to_dataframe()
    
    #convert to list to easily loop through the data                          
    tables = tables[tables['table_id'].str.contains(listing_type)]['table_id'].tolist()
    
    
    #GENERATE RELEVANT DATA FROM EACH TABLE

    df = pd.DataFrame()

    for i, table in enumerate(tables):
        data = client.query(f"""
                            SELECT id, created_on, operation, property_type, place_name, country_name, state_name,
                            lat, lon, price_aprox_usd, surface_total_in_m2, surface_covered_in_m2,
                            price_usd_per_m2, floor, rooms, expenses
                            FROM `properati-data-public.properties_br.{table}`
                            """).result()
        
        data_df = data.to_dataframe() #convert data to dataframe
        data_df['table_id'] = table #append the table_id for reference
        
        df = pd.concat([data_df, df])
        print(f'Extracted data from {table} table. Tables remaining: {len(tables) - i - 1}. ', end = '\r')
    
    time.sleep(3)
    
    print('Completed!                                                                                     ')
    
    return df

    time.sleep(2)

# %% RUN QUERY

rent_df = query_table('rent')
sell_df = query_table('sell')


# %% ENDNOTES

end = timer()

print(f'\n*****\n\nData extraction completed successfully! Runtime: {round((end-start)/60,2)} minutes\n')
time.sleep(2)

print('*****\n\nDataframes created:')
time.sleep(2)

print(f'\nrent_df - contains {len(rent_df):,} real estate listings for rent')
time.sleep(2)

print(f'sell_df - contains {len(sell_df):,} real estate listings for sale')
time.sleep(2)
print('\n===============================================================================================================================')

# print('Saving CSV files...')
# rent_df.to_csv('Dataset/Raw/Brazil Listing (Rent).csv')
# sell_df.to_csv('Dataset/Raw/Brazil Listing (Sell).csv')


# %% 