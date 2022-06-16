import argparse
import datetime
from dateutil.parser import parse
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import struct
from pyspark.sql.functions import collect_list

def ignite():
    spark = (SparkSession
            .builder
            .config("hive.input.dir.recursive", "true")
            .config("hive.mapred.supports.subdirectories", "true")
            .config("hive.supports.subdirectories", "true")
            .config("mapred.input.dir.recursive", "true") 
            .config("spark.sql.sources.partitionOverwriteMode","dynamic")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("hive.exec.dynamic.partition", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.shuffle.partitions", 1200)
            .enableHiveSupport()
            .getOrCreate())
    return spark

# get dataframe with minute-level placeholder
def get_filled_price(source, date, df_joined_token):
    query = """select name,symbol,price,from_unixtime(cast(timestamp as int)) timestamp,data_creation_date
            from solana_mainnet.%(source)s where data_creation_date='%(date)s'
            group by name,symbol,price,timestamp,data_creation_date order by timestamp;"""
    query_vars = {'source': source, 'date': date}
    df = spark.sql(query % query_vars)

    df_token = df.dropDuplicates(['name','symbol']).select(['name','symbol'])
    df_token_list = df_joined_token.alias('a')
    df_token = df_token.alias('b')
    df_token = df_token.join(df_token_list, ['name','symbol'])\
        .orderBy(["symbol","name"], ascending=[1,1])
    print(df_token.count())

    minute = datetime.timedelta(minutes=1)
    MINUTES_PER_DAY = 1440
    def get_minutes(date):
        timestamps = []
        timestamps.append(parse(date).replace(tzinfo=datetime.timezone.utc))
        while len(timestamps) < MINUTES_PER_DAY:
            timestamps.append(timestamps[-1] + minute)
        return timestamps

    timestamps = get_minutes(date)
    df_minute = sc.parallelize(timestamps).map(lambda x: (x,)).toDF(['minute'])

    df_holder = df_minute.crossJoin(df_token)

    df_price = df.alias('a')
    df_holder = df_holder.alias('b')
    df_joined = df_price.join(df_token, ['name', 'symbol'])\
        .join(df_holder, (df_price.timestamp == df_holder.minute) & (df_price.name == df_holder.name) & (df_price.symbol == df_holder.symbol), 'outer')\
        .select(['b.name', 'b.symbol', 'a.price', 'a.timestamp', 'b.minute', 'a.data_creation_date'])\
        .orderBy(["symbol","name","minute"], ascending=[1,1,1])
    df_joined.show(10)
    return df_joined


def generate_price(date):
    query_joined_token = """select a.name,a.symbol from (select * from prices.crypto_metadata where source='coincap') a join
        (select * from prices.crypto_metadata where source='coinmarketcap') b on a.name=b.name and a.symbol=b.symbol
        group by 1,2"""
    df_joined_token = spark.sql(query_joined_token)
    print(df_joined_token.count())
    df_joined_token.printSchema()

    source = 'cryptocurrency_price_in_usd'
    df_joined_cmc = get_filled_price(source, date, df_joined_token)
    print(df_joined_cmc.count())
    df_joined_coincap = get_filled_price(source+'_coincap', date, df_joined_token)
    print(df_joined_coincap.count())

    df_cmc = df_joined_cmc.alias('cmc')
    df_coincap = df_joined_coincap.alias('cap')
    df_joined_prices = df_cmc.join(df_coincap,['name','symbol','minute'])\
        .selectExpr('name','symbol','cmc.price as price_cmc', 'cap.price as price_cap', 'minute', 'cap.data_creation_date')\
        .orderBy(["symbol","name","minute"], ascending=[1,1,1])
    df_joined_prices.printSchema()
    print(df_joined_prices.count())
    
    df_grouped_joined_prices = df_joined_prices.groupBy(['name','symbol'])\
        .agg(collect_list(struct(['price_cmc','price_cap','minute'])).alias('price_info'))
    df_grouped_joined_prices.printSchema()
    
    price_source_columns = ['price_cmc', 'price_cap']
    def transform(sqlrow):
        row = sqlrow.asDict()
        price_info = row['price_info']  
        price_info.sort(key=lambda t: t['minute'])
        first_not_null_price_index = {col:-1 for col in price_source_columns}
        last_price = {col:None for col in price_source_columns}
        for i in range(len(price_info)):
            price_row = price_info[i].asDict()
            for src in price_source_columns:
                if price_row[src]:
                    last_price[src] = price_row[src]
                    if first_not_null_price_index[src] < 0:
                        first_not_null_price_index[src] = i
                else:
                    price_row[src] = last_price[src]
            price_info[i] = price_row
        for src in price_source_columns:
            to_index = first_not_null_price_index[src]
            if to_index >= 0:
                first_price = price_info[to_index][src]
                for i in range(to_index):
                    price_info[i][src] = first_price
            else:
                print(f"{row['name']}|{row['symbol']} {src} no value")
        new_rows = []
        avg_price_w_source = {src: 0.0 for src in price_source_columns}
        for price_row in price_info:
            sum_price = 0.0
            for src in price_source_columns:
                sum_price += price_row[src]
                avg_price_w_source[src] += price_row[src]
            avg_price = sum_price / len(price_source_columns)
            new_row = {
                'name': row['name'],
                'symbol': row['symbol'],
                'price': avg_price,
                'minute': price_row['minute'],
                'data_creation_date': date
            }
            new_rows.append(Row(**new_row))

        price_1 = avg_price_w_source[price_source_columns[0]]
        price_2 = avg_price_w_source[price_source_columns[1]]
        difference = abs(price_1 - price_2) / price_1
        if difference < 0.1:
            # pass check
            return new_rows
        return []

    rdd_grouped_joined_prices_processed = df_grouped_joined_prices.rdd.flatMap(transform)
    df_grouped_joined_prices_processed = rdd_grouped_joined_prices_processed.toDF()
    df_grouped_joined_prices_processed.printSchema()
    print(df_grouped_joined_prices_processed.count())
    
    df_grouped_joined_prices_processed.repartition(1).write.mode('append').partitionBy('data_creation_date').parquet('s3://parquet-intermediate/prices/usd/')

def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('date', metavar='date', type=str,
                        help='date to check')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = argument_parser()
    print(args)
    date = parse(args.date).strftime('%Y-%m-%d')

    sc = SparkContext(appName='crypto_price_job')
    spark = ignite()
    sqlContext = SQLContext(sc)

    generate_price(date)
