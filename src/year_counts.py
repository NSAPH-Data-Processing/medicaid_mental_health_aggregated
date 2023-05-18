## Load packages ----
import numpy as np
import pandas as pd
import sshtunnel
import psycopg2 as pg
import os
import seaborn as sns
import matplotlib.pyplot as plt
import json

## Open ssh tunnel to DB host ----
tunnel = sshtunnel.SSHTunnelForwarder(
    ('nsaph.rc.fas.harvard.edu', 22),
    ssh_username=f'{os.environ["MY_NSAPH_SSH_USERNAME"]}',
    ssh_private_key=f'{os.environ["HOME"]}/.ssh/id_rsa', 
    ssh_password=f'{os.environ["MY_NSAPH_SSH_PASSWORD"]}', 
    remote_bind_address=("localhost", 5432)
)

tunnel.start()

## Open connection to DB ----
connection = pg.connect(
    host='localhost',
    database='nsaph2',
    user=f'{os.environ["MY_NSAPH_DB_USERNAME"]}',
    password=f'{os.environ["MY_NSAPH_DB_PASSWORD"]}', 
    port=tunnel.local_bind_port
)

def get_counts_query(diagnoses):
    """
    calculates the number of mental health hospitalizations per year
    """
    sql_query = f"""
    WITH hosp AS ( 
        SELECT 
            year, 
            COUNT(bene_id) AS n 
        FROM 
            medicaid.admissions
        WHERE 
            diagnosis::text[] && ARRAY[{diagnoses}]
        GROUP BY
            year
    ), 
    y AS(
        SELECT DISTINCT year
        FROM medicaid.admissions
    )
    SELECT 
        y.year, 
        COALESCE(hosp.n, 0) AS hospitalizations
        FROM y LEFT JOIN hosp ON y.year = hosp.year
    """
    return sql_query

def main():

    diagnoses = json.load(open('../icd_codes.json'))
    dfs = []

    for key in diagnoses:
        sql_query= get_counts_query(diagnoses[key]['icd9'])
        print("***************************************")
        print(key)
        print(sql_query)
        print("***************************************")
        df = pd.read_sql_query(sql_query, connection)
        df['outcome'] = key
        dfs.append(df)
        #break

    print("## store results ----")
    result_df = pd.concat(dfs)  # Concatenate all the DataFrames into one
    result_df.to_csv("../data/year_counts.csv", index=False)
    
    print(result_df)
        
if __name__ == '__main__':
    main()
