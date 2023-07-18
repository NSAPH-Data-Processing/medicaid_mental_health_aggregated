## Load packages ----
import pandas as pd
import sshtunnel
import psycopg2 as pg
import os
import json
import argparse
import time

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
            CASE 
                WHEN b.sex LIKE '%,%' THEN 'U'
                ELSE b.sex
                END AS sex_,
            CASE 
                WHEN b.race_ethnicity_code LIKE '%,%' THEN '9'
                ELSE b.race_ethnicity_code
                END AS race_,
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
            extract(year from a.admission_date) = '{year}'AND
            a.diagnosis::text[] && ARRAY[{diagnoses}]
        -- LIMIT 100
    ),
    adm2 AS (
        SELECT
            a.bene_id,
            a.year,
            a.month,
            b.state,
            b.residence_county,
            b.sex_,
            b.race_,
            CASE 
                WHEN b.age < 19 THEN '0-18'
                WHEN b.age >= 19 AND b.age < 25 THEN '19-24'
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
        sex_,
        race_,
        age_group,
        count(bene_id) as hospitalizations
    FROM
        adm2
    GROUP BY
        year,
        month,
        state,
        residence_county,
        sex_,
        race_,
        age_group
    """
    return sql_query

def get_all_hospitalizations(year):
    """
    computes counts of all cause hospitalizations per county and strata
    """
    sql_query = f"""
    WITH bene AS (
        SELECT 
            e.bene_id,
            e.year,
            e.state,
            e.fips5 as residence_county,
            CASE 
                WHEN b.sex LIKE '%,%' THEN 'U'
                ELSE b.sex
                END AS sex_,
            CASE 
                WHEN b.race_ethnicity_code LIKE '%,%' THEN '9'
                ELSE b.race_ethnicity_code
                END AS race_,
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
            extract(year from a.admission_date) = '{year}'
        -- LIMIT 100
    ),
    adm2 AS (
        SELECT
            a.bene_id,
            a.year,
            a.month,
            b.state,
            b.residence_county,
            b.sex_,
            b.race_,
            CASE 
                WHEN b.age < 19 THEN '0-18'
                WHEN b.age >= 19 AND b.age < 25 THEN '19-24'
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
        sex_,
        race_,
        age_group,
        count(bene_id) as all_cause_hospitalizations
    FROM
        adm2
    GROUP BY
        year,
        month,
        state,
        residence_county,
        sex_,
        race_,
        age_group
    """
    return sql_query

def main(args):

    print("## Connecting to database ----")
    print("## Open ssh tunnel to DB host ----")

    success = False
    attempts = 0

    while not success:
        try:
            tunnel = sshtunnel.SSHTunnelForwarder(
                ('nsaph.rc.fas.harvard.edu', 22),
                ssh_username=f'{os.environ["MY_NSAPH_SSH_USERNAME"]}',
                ssh_private_key=f'{os.environ["HOME"]}/.ssh/id_rsa', 
                ssh_password=f'{os.environ["MY_NSAPH_SSH_PASSWORD"]}', 
                remote_bind_address=("localhost", 5432)
            )

            tunnel.start()

            print("## Open connection to DB ----")
            connection = pg.connect(
                host='localhost',
                database='nsaph2',
                user= f'{os.environ["MY_NSAPH_DB_USERNAME"]}',
                password=f'{os.environ["MY_NSAPH_DB_PASSWORD"]}', 
                port=tunnel.local_bind_port
            )
            success = True
        except:
            attempts += 1
            print(f"## Failed to connect to DB, attempt {attempts} ----")
            if attempts > 10:
                raise Exception("## Failed to connect to DB ----")
            time.sleep(5)

    total_data = []

    print("# get all cause hospitalizations ----")
    sql_query = get_all_hospitalizations(args.year)
    print(sql_query)
    data = pd.read_sql_query(sql_query, connection)
    total_data.append(data)

    print("# get mental_health counts ----")
    # get diagnoses ----
    diagnoses = json.load(open(args.icd_json, 'r'))

    for key in diagnoses:
        sql_query= get_counts_query(diagnoses[key]['icd9'], args.year)
        print("***************************************")
        print(key)
        print(sql_query)
        print("***************************************")
        data = pd.read_sql_query(sql_query, connection)

        data.rename(columns={'hospitalizations':f"{key}_hospitalizations"}, inplace=True)
        #data.dropna(subset=['state','residence_county', 'admission_date'], inplace=True)
        total_data.append(data)

    print("# merge dataframes ----")
    # Initial DataFrame
    df = total_data[0]

    # Iterate over the remaining DataFrames and merge with the accumulated df
    for data in total_data[1:]:
        df = pd.merge(df, data, how = 'outer', on=['year', 'month', 'state', 'residence_county', 'sex_', 'race_', 'age_group'])

    ## == Format output == ##
    # typecast columns
    df['year'] = df.year.astype(int)
    df['month'] = df.month.astype(int)

    df['state'] = df.state.astype(str)

    df.rename(columns={'sex_': 'sex', 'race_': 'race'}, inplace=True)
    df['sex'] = df.sex.astype(str)
    df['race'] = df.race.astype(int)

    df['age_group'] = df.age_group.astype(str)

    columns = df.columns.tolist()
    hospitalizations_columns = [c for c in columns if 'hospitalizations' in c]
    for c in hospitalizations_columns:
        df[c].fillna(0, inplace=True)
        df[c] = df[c].astype('int')
    df['all_cause_hospitalizations'] = df['all_cause_hospitalizations'].astype('int')
    
    # Pad leading zeros from residence_county to have 5 digits in each code
    df = df[df['residence_county'].notna()]
    df['residence_county'] = df['residence_county'].astype(int).astype(str).str.zfill(5)

    # # Replace entries containing commas with NaN
    # df['sex'] = df['sex'].replace({r'.*,.*': ''}, regex=True)
    # df['race'] = df['race'].replace({r'.*,.*': float('nan')}, regex=True)

    print("Close the connection and tunnel ----")
    connection.close()
    tunnel.stop()

    print("## Writing output ----")
    df = df.set_index(['residence_county'])

    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file)
    elif args.output_format == "csv":
        df.to_csv(output_file)

    print(f"## Output written to {output_file}")

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
    parser.add_argument("--output_format", 
                        default = "csv", 
                        choices=["parquet", "csv"]
                       ) 
    parser.add_argument('--output_prefix', type=str, help='output file prefix', 
        default='../data/output/medicaid_mental_health/mental_health_hospitalizations')
    args = parser.parse_args()

    main(args)
