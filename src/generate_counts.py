## Load packages ----
import pandas as pd
import sshtunnel
import psycopg2 as pg
import os
import json
import argparse

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

def get_counts_query(diagnoses, year):
    """
    computes counts of mental health hospitalizations per county and strata
    """
    sql_query = f"""
    WITH bene AS (
        SELECT 
            e.bene_id,
            e.year,
            e.state,
            e.fips5 as residence_county,
            b.sex,
            b.race_ethnicity_code as race,
            e.year - EXTRACT(YEAR FROM b.dob) as age
        FROM
            medicaid.enrollments as e
        LEFT JOIN
            medicaid.beneficiaries as b
        USING (bene_id)
        WHERE
            e.year = '{year}'
    ), 
    adm1 AS (
        SELECT
            a.bene_id,
            extract(year from a.admission_date) as year,
            extract(month from a.admission_date) as month
        FROM
            medicaid.admissions as a
        WHERE
            extract(year from a.admission_date) = '{year}' AND
            a.diagnosis::text[] && ARRAY[{diagnoses}]
        --LIMIT 100
    ),
    adm2 AS (
        SELECT
            a.bene_id,
            a.year,
            a.month,
            b.state,
            b.residence_county,
            b.sex,
            b.race,
            CASE 
                WHEN b.age < 18 THEN '0-17'
                WHEN b.age >= 18 AND b.age < 25 THEN '18-24'
                WHEN b.age >= 25 AND b.age < 35 THEN '25-34'
                WHEN b.age >= 35 AND b.age < 45 THEN '35-44'
                WHEN b.age >= 45 AND b.age < 55 THEN '45-54'
                WHEN b.age >= 55 AND b.age < 65 THEN '55-64'
                WHEN b.age >= 65 AND b.age < 75 THEN '65-74'
                WHEN b.age >= 75 AND b.age < 85 THEN '75-84'
                WHEN b.age >= 85 THEN '85+'
                ELSE 'NA'
            END AS age_group
        FROM
            adm1 as a
        LEFT JOIN
            bene as b
        USING (bene_id, year)
    )
    SELECT 
        year,
        month,
        state,
        residence_county,
        sex,
        race,
        age_group,
        count(bene_id) as hospitalizations
    FROM
        adm2
    GROUP BY
        year,
        month,
        state,
        residence_county,
        sex,
        race,
        age_group
    """
    return sql_query

def main(args):
    print("# get diagnoses ----") 
    diagnoses = json.load(open(args.icd_json, 'r'))
    
    print("# get counts ----")
    total_df = []
    for key in diagnoses:
        sql_query= get_counts_query(diagnoses[key]['icd9'], args.year)
        print("***************************************")
        print(key)
        print(sql_query)
        print("***************************************")
        data = pd.read_sql_query(sql_query, connection)

        data.rename(columns={'hospitalizations':f"{key}_hospitalizations"}, inplace=True)
        #data.dropna(subset=['state','residence_county', 'admission_date'], inplace=True)
        total_df.append(data)

    print("# merge dataframes ----")
    # Initial DataFrame
    merged_df = total_df[0]

    # Iterate over the remaining DataFrames and merge with the accumulated merged_df
    for df in total_df[1:]:
        merged_df = pd.merge(merged_df, df, how = 'outer', on=['year', 'month', 'state', 'residence_county', 'sex', 'race', 'age_group'])

    ## == Format output == ##
    # typecast columns
    merged_df['year'] = merged_df.year.astype(int)
    merged_df['month'] = merged_df.month.astype(int)
    columns = merged_df.columns.tolist()
    hospitalizations_columns = [c for c in columns if 'hospitalizations' in c]
    for c in hospitalizations_columns:
        merged_df[c].fillna(0, inplace=True)
        merged_df[c] = merged_df[c].astype('int')
    
    # Replace entries containing commas with NaN
    merged_df['sex'] = merged_df['sex'].replace({r'.*,.*': ''}, regex=True)
    merged_df['race'] = merged_df['race'].replace({r'.*,.*': float('nan')}, regex=True)

    print("# write output ----")
    merged_df.to_csv(f'{args.output_prefix}_{args.year}.csv', index=False)

    # Close the connection
    connection.close()
    print("complete")

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='Generate counts of mental health hospitalizations per county per year')
    parser.add_argument(
        '--year', type=int, help='year to generate counts for',
        default=2012
        )
    parser.add_argument(
        '--icd_json', type=str, help='path to json file containing diagnoses to include', 
        default='../data/input/icd_codes.json'
        )
    parser.add_argument('--output_prefix', type=str, help='output file prefix', 
        default='../data/output/scratch/mental_health_hospitalizations')
    args = parser.parse_args()

    main(args)
