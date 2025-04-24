#!/usr/bin/env python
# coding: utf-8

import argparse, os, sys
from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name}...')
    os.system(f"wget {url.strip()} -O {file_name}")
    print('\n')


    # Creating a connection to Postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    
    else:
        print('Error. Only .csv or .parquet files are allowed.')
        sys.exit()

    
    # Create the table.
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the values in the database
    t_start = time()
    count = 0

    for batch in file.iter_batches(batch_size=100000):
        count += 1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        
        print(f'Inserting batch {count}...')

        b_start = time()

        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()

        print(f'Inserted! Time taken: {b_end - b_start:10.3f} seconds \n')

    t_end = time()
    print(f'Completed! Total time taken was {t_end - t_start:10.3f} seconds for {count} batches.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')


    # user, password, host, port, database name, table name, url of the CSV
    parser.add_argument('--user', help='Username for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--db', help='Database for postgres')
    parser.add_argument('--table_name', help='Name of the table where we will write the results to')
    parser.add_argument('--url', help='URL of the CSV file')

    args = parser.parse_args()

    main(args)