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


def get_outcomes():
    """ Get and return ICD codes """""
    f = open('icd_codes.json')
    outcomes_ = json.load(f)
    f.close()
    return json.loads(outcomes_[0])

def get_counts_query(diagnoses, year):
    """
    calculates the number of mental health hospitalizations per county per year
    """
    sql_query = f"""
    WITH d AS ( 
    SELECT DISTINCT
        admission_date, 
        bene_id
    FROM 
        medicaid.admissions
    WHERE 
        diagnosis::text[] && ARRAY[{diagnoses}]
        AND admission_date >= '{year}-01-01' AND admission_date < '{year + 1}-01-01'
    ), 
    y AS (
        SELECT DISTINCT 
            e.state,
            e.residence_county, 
            d.admission_date,
            COUNT(DISTINCT d.bene_id) AS hospitalizations
        FROM 
            medicaid.enrollments e 
            JOIN d ON e.bene_id = d.bene_id
        GROUP BY 
            e.state,
            e.residence_county, 
            d.admission_date
    )
    SELECT
        y.residence_county, 
        y.state,
        y.admission_date,
        y.hospitalizations
    FROM 
        y

        """
    
    
    return sql_query


diagnoses = get_outcomes()
for key in diagnoses:
    for year in range(1999,2013):
        sql_query= get_counts_query(diagnoses[key]['icd9'], year)
        print("***************************************")
        print(key)
        print(year)
        print(sql_query)
        print("***************************************")
        data = pd.read_sql_query(sql_query, connection)
        filename = f"data/state/{key}_diagnoses_per_county_per_day_{year}_state.csv"
        data.to_csv(filename)
        
        
print("complete")