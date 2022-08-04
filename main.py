import uuid
import pandas as pd

df0 = pd.read_csv('/Users/crystalli/Documents/Senior/AmazonPTA/DataFile.csv')


def create_uuid(df):
    run_id = uuid.uuid4()
    return run_id


df0['UUID'] = create_uuid(df0)


def cast_data_type(df):
    df.astype({"target_label": int, "ASIN_STATIC_ITEM_PACKAGE_WEIGHT": float, "ASIN_STATIC_LIST_PRICE": float})
    return df


df0 = cast_data_type(df0)


def change_nan_to_none(df):
    df.where(pd.notnull(df), None)
    return df


df0 = change_nan_to_none(df0)

import psycopg2
from psycopg2 import Error

# Set up connection to Postgresql
try:
    conn = psycopg2.connect(
        host="localhost",
        database="suppliers",
        user="crystalli",
        password="5432")
    cursor = conn.cursor()
    print("PostgreSQL server information")
    print(conn.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

try:
    # Dropping table asin_electrical_plug if exists
    cursor.execute("DROP TABLE IF EXISTS asin_electrical_plug;")
    # Creating table asin_electrical_plug
    sql = '''CREATE TABLE asin_electrical_plug(
        asin VARCHAR(20) NOT NULL, 
        target_label INT NOT NULL, 
        asin_static_item_name VARCHAR(50) NOT NULL, 
        asin_static_product_description VARCHAR(300),            
        asin_static_gl_product_group_type VARCHAR(50),
        asin_static_item_package_weight FLOAT8,
        asin_static_list_price FLOAT8,
        asin_static_batteries_included BOOL,
        asin_static_batteries_required BOOL,
        asin_static_item_classification VARCHAR(50),
        uuid VARCHAR(50)
        )'''
    cursor.execute(sql);
    print("asin_electrical_plug table is created successfully...............")
except OperationalError as err:
    # pass exception to function
    show_psycopg2_exception(err)
    # set the connection to 'None' in case of error
    conn = None


# Insert the modified dataframe into Postgresql database
def execute_values(conn, datafrm, table):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        print("Data inserted using execute_values() successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        cursor.close()


# Perform insertion
execute_values(conn, df0, 'asin_electrical_plug')
