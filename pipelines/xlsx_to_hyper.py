import os

import findspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Nullability,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
    escape_name,
    escape_string_literal,
)
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


def write_to_hdfs_specify_path(df, spark, hdfs_path, file_name):
    """
    :param df: dataframe which you want to save
    :param spark: sparkSession
    :param hdfs_path: target path(shoul be not exises)
    :param file_name: csv file name
    :return:
    """
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    df.coalesce(1).write.option('header', True).option('delimiter', ',').option('compression', 'none').mode(
        'overwrite'
    ).csv(hdfs_path, escape='"')
    fs = FileSystem.get(Configuration())
    file = fs.globStatus(Path('%s/part*' % hdfs_path))[0].getPath().getName()
    full_path = '%s/%s' % (hdfs_path, file_name)
    result = fs.rename(Path('%s/%s' % (hdfs_path, file)), Path(full_path))
    return result


def spark_preprocess_to_csv():
    print('Converting data to csv...')
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

    # Export to csv
    # orders_df_combined.write.option('header', 'true').mode(write_mode).csv(f'{os_path}/../data/output/orders.csv')
    write_to_hdfs_specify_path(orders_df_combined, spark, f'{os_path}/../data/output/orders.csv', 'orders.csv')
    print('  Orders written to csv')
    # customers_df_combined.write.option('header', 'true').mode(write_mode).csv(f'{os_path}/../data/output/customers.csv')
    write_to_hdfs_specify_path(customers_df_combined, spark, f'{os_path}/../data/output/customers.csv', 'customers.csv')
    print('  Customers written to csv')
    # products_df.write.option('header', 'true').mode(write_mode).csv(f'{os_path}/../data/output/products.csv')
    write_to_hdfs_specify_path(products_df, spark, f'{os_path}/../data/output/products.csv', 'products.csv')
    print('  Products written to csv')


def insert_from_csv(conn, table_def, csv_path):
    conn.catalog.create_table(table_def)
    conn.execute_command(
        f'COPY {table_def.table_name} from {escape_string_literal(csv_path)} with '
        f"(format csv, NULL 'NULL', delimiter ',', header)"
    )


def create_hyper_from_csv(ignore=False):
    print('Creating Hyper file...')
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint,
            os_path + '/../data/output/gearquest.hyper',
            (CreateMode.CREATE_IF_NOT_EXISTS if ignore else CreateMode.CREATE_AND_REPLACE),
        ) as conn:
            # Create schema
            conn.catalog.create_schema('gearquest')

            orders_table_def = TableDefinition(
                TableName('gearquest', 'orders'),
                [
                    TableDefinition.Column('order_id', SqlType.varchar(255), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('customer_id', SqlType.varchar(255), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('product_id', SqlType.varchar(255), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('currency', SqlType.varchar(255)),
                    TableDefinition.Column('local_price', SqlType.numeric(10, 2)),
                    TableDefinition.Column('usd_price', SqlType.numeric(10, 2)),
                    TableDefinition.Column('purchase_platform', SqlType.varchar(255)),
                    TableDefinition.Column('purchase_ts', SqlType.varchar(255)),
                    TableDefinition.Column('ship_ts', SqlType.varchar(255)),
                    TableDefinition.Column('delivery_ts', SqlType.varchar(255)),
                    TableDefinition.Column('refund_ts', SqlType.varchar(255)),
                ],
            )
            customers_table_def = TableDefinition(
                TableName('gearquest', 'customers'),
                [
                    TableDefinition.Column('customer_id', SqlType.varchar(255), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('marketing_channel', SqlType.varchar(255)),
                    TableDefinition.Column('account_creation_method', SqlType.varchar(255)),
                    TableDefinition.Column('created_on', SqlType.varchar(255)),
                    TableDefinition.Column('country', SqlType.varchar(255)),
                    TableDefinition.Column('region', SqlType.varchar(255)),
                    TableDefinition.Column('currency', SqlType.varchar(255)),
                ],
            )
            products_table_def = TableDefinition(
                TableName('gearquest', 'products'),
                [
                    TableDefinition.Column('product_id', SqlType.varchar(255), Nullability.NOT_NULLABLE),
                    TableDefinition.Column('product_name', SqlType.varchar(255)),
                    TableDefinition.Column('category', SqlType.varchar(255)),
                    TableDefinition.Column('specs', SqlType.varchar(255)),
                    TableDefinition.Column('price_USD', SqlType.int()),
                ],
            )

            # Table Insertion
            insert_from_csv(conn, orders_table_def, os_path + '/../data/output/orders.csv/orders.csv')
            print('  Orders written to Hyper')
            insert_from_csv(conn, customers_table_def, os_path + '/../data/output/customers.csv/customers.csv')
            print('  Customers written to Hyper')
            insert_from_csv(conn, products_table_def, os_path + '/../data/output/products.csv/products.csv')
            print('  Products written to Hyper')


if __name__ == '__main__':
    convert_data(ignore=True)
    spark_preprocess_to_csv()
    create_hyper_from_csv()
    print('Done!')
