import os

import findspark
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from xlsx2csv import Xlsx2csv

os_path = os.path.dirname(os.path.abspath(__file__))


def convert_xlsx_to_csv(xlsx_path, csv_path, ignore):
    print(f'Converting {xlsx_path.split("/")[-1]}')
    if os.path.exists(csv_path) & ignore:
        print(f'  {csv_path.split("/")[-1]} already exists. Skipping conversion...')
        return
    Xlsx2csv(xlsx_path, outputencoding='utf-8').convert(csv_path)
    print(f'{xlsx_path.split("/")[-1]} converted to {csv_path.split("/")[-1]}')


def convert_data(ignore=False):
    convert_xlsx_to_csv(f'{os_path}/../data/raw/Orders.xlsx', f'{os_path}/../data/inter/Orders.csv', ignore)
    convert_xlsx_to_csv(f'{os_path}/../data/raw/Order_Status.xlsx', f'{os_path}/../data/inter/Order_Status.csv', ignore)
    convert_xlsx_to_csv(f'{os_path}/../data/raw/Products.xlsx', f'{os_path}/../data/inter/Products.csv', ignore)
    convert_xlsx_to_csv(f'{os_path}/../data/raw/Geo_lookup.xlsx', f'{os_path}/../data/inter/Geo_lookup.csv', ignore)
    convert_xlsx_to_csv(f'{os_path}/../data/raw/Customers.xlsx', f'{os_path}/../data/inter/Customers.csv', ignore)


def spark_read_csv(spark, csv_path):
    return spark.read.option('header', 'true').option('multiLine', 'true').option('escape', '"').csv(csv_path)


def create_table(cursor, table_name, columns):
    column_definitions = ', '.join([f'"{name}" {dtype}' for name, dtype in columns])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_definitions})')


def spark_preprocess_to_postgres():
    print('Processing & syncing data to postgres...')
    # Initialize spark
    findspark.init()
    spark = (
        SparkSession.builder.appName('GearQuest ETL Pipeline')
        .config('spark.sql.repl.eagerEval.enabled', 'true')
        .getOrCreate()
    )

    # Load data
    orders_df = spark_read_csv(spark, f'{os_path}/../data/inter/Orders.csv')
    order_status_df = spark_read_csv(spark, f'{os_path}/../data/inter/Order_Status.csv')
    products_df = spark_read_csv(spark, f'{os_path}/../data/inter/Products.csv')
    geo_lookup_df = spark_read_csv(spark, f'{os_path}/../data/inter/Geo_lookup.csv')
    customers_df = spark_read_csv(spark, f'{os_path}/../data/inter/Customers.csv')

    # Drop duplicates
    orders_df = orders_df.dropDuplicates()
    order_status_df = order_status_df.dropDuplicates()
    products_df = products_df.dropDuplicates()
    geo_lookup_df = geo_lookup_df.dropDuplicates()
    customers_df = customers_df.dropDuplicates()

    # Combine Orders and Order Status
    orders_df_combined = (
        orders_df.drop('purchase_ts')
        .join(order_status_df, orders_df['id'] == order_status_df['order_id'], 'left')
        .drop('order_id')
        .withColumnRenamed('id', 'order_id')
        .sort('order_id')
    )

    # Combine Customers and Geo Lookup
    customers_df_combined = (
        customers_df.join(geo_lookup_df, customers_df['country_code'] == geo_lookup_df['country'], 'left')
        .drop('country_code')
        .withColumnRenamed('id', 'customer_id')
        .sort('customer_id')
    )

    # Handle nulls and convert data types
    orders_df_combined = orders_df_combined.withColumn(
        'local_price', col('local_price').cast('decimal(10,2)')
    ).withColumn('usd_price', col('usd_price').cast('decimal(10,2)'))

    products_df = products_df.sort('product_id')

    # Sync to Postgres

    # Load environment variables
    load_dotenv()
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')
    pg_database = os.getenv('PG_DATABASE')

    print(f'  Connecting to {pg_host} as {pg_user}...')
    print(f'  Using database {pg_database}...')

    # Connect to PostgreSQL
    conn = psycopg2.connect(host=pg_host, user=pg_user, password=pg_password, database=pg_database)
    cursor = conn.cursor()

    # Define table schemas and primary keys
    table_schemas = {
        'orders': {
            'columns': [
                ('index', 'INTEGER NOT NULL PRIMARY KEY'),  # PRIMARY KEY
                ('order_id', 'TEXT NOT NULL'),
                ('customer_id', 'TEXT NOT NULL'),
                ('product_id', 'TEXT NOT NULL'),
                ('currency', 'TEXT'),
                ('local_price', 'NUMERIC(10, 2)'),
                ('usd_price', 'NUMERIC(10, 2)'),
                ('purchase_platform', 'TEXT'),
                ('purchase_ts', 'TEXT'),
                ('ship_ts', 'TEXT'),
                ('delivery_ts', 'TEXT'),
                ('refund_ts', 'TEXT'),
            ],
            'dataframe': orders_df_combined,
        },
        'customers': {
            'columns': [
                ('customer_id', 'TEXT NOT NULL PRIMARY KEY'),  # PRIMARY KEY
                ('marketing_channel', 'TEXT'),
                ('account_creation_method', 'TEXT'),
                ('created_on', 'TEXT'),
                ('country', 'TEXT'),
                ('region', 'TEXT'),
                ('currency', 'TEXT'),
            ],
            'dataframe': customers_df_combined,
        },
        'products': {
            'columns': [
                ('product_id', 'TEXT NOT NULL PRIMARY KEY'),  # PRIMARY KEY
                ('product_name', 'TEXT'),
                ('category', 'TEXT'),
                ('price_USD', 'INTEGER'),
            ],
            'dataframe': products_df,
        },
    }

    pg_url = f'jdbc:postgresql://{pg_host}/{pg_database}'
    pg_properties = {
        'user': pg_user,
        'password': pg_password,
        'driver': 'org.postgresql.Driver',
        'batchsize': '10000',
    }

    # Create each table if not already created
    for table_name, schema in table_schemas.items():
        create_table(cursor=cursor, table_name=table_name, columns=schema['columns'])
        print(f'    {table_name}: Table exists or has been created...')

    # Sync each table with the database
    for table_name, schema in table_schemas.items():
        df = schema['dataframe']
        df.write.jdbc(url=pg_url, table=table_name, mode='overwrite', properties=pg_properties)
        print(f'    {table_name}: Data synced with the database...')

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    convert_data(ignore=True)
    spark_preprocess_to_postgres()
    print('Done!')
