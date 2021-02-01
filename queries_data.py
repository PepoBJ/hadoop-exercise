from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *


def accumulated_delay_column(partition_column, order_column):
    window_partiton = Window.partitionBy(col(partition_column)).orderBy(col(order_column))
    return sum(col(order_column)).over(window_partiton)

def vip_country(country_code):
    return '1' if (country_code == 'ESP') | (country_code == 'PER') else '0'

if __name__ == "__main__":
    print('Initializing job...')
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sql('use air_tribu')

    flight_history_df = spark.table('t_flight_history')

    print('> The countries with the most take-offs:')
    take_off_df = flight_history_df.groupBy('country_origin_code').agg(count('*').alias('take_off')) \
                    .select(col('country_origin_code').alias('country_code'), col('take_off')) \
                    .orderBy(desc('take_off'))
    take_off_df.show(5, False)

    print('> The countries with the most arrivals:')
    arrive_df =  flight_history_df.groupBy('country_target_code').agg(count('*').alias('arrive')) \
                    .select(col('country_target_code').alias('country_code'), col('arrive')) \
                    .orderBy(desc('arrive'))
    arrive_df.show(5, False)

    print('> Consolidated table with take-offs and arrivals:')
    country_df = spark.table('t_country')

    take_arrive_country_df = country_df.join(take_off_df, on='country_code', how='left') \
                            .join(arrive_df, on='country_code', how='left') \
                            .orderBy('country_name')
    take_arrive_country_df.show(50, False)

    print('> The days with more and less flights:')
    number_flight_df = flight_history_df.groupBy('day_number').agg(count('*').alias('number_flight')) \
                        .select(col('day_number'), col('number_flight')) \
                        .orderBy(desc('number_flight'))
    number_flight_df.show(5, False)

    print('> The days with the most and least delays:')
    flight_time_df = flight_history_df.groupBy('day_number').agg(avg('delay_time').alias('delay_time')) \
                        .select(col('day_number'), col('delay_time')) \
                        .orderBy(desc('delay_time'))
    flight_time_df.show(5, False)

    print('> Consolidated table with flights and delays:')
    number_flight_time_df = number_flight_df.join(flight_time_df, on='day_number', how='full')
    number_flight_time_df.show(50, False)

    print('> [+] Consolidated table with countries with delayed flights:')
    country_delay_flight_df = flight_history_df.groupBy('country_origin_code', 'country_origin_name') \
                                .agg(avg('delay_time').alias('delay_time')) \
                                .select(col('country_origin_code'), col('country_origin_name'), col('delay_time')) \
                                .orderBy(desc('delay_time'))
    country_delay_flight_df.show(50, False)

    print('> [Additional] Accumulated delay:')
    accumulated_delay_df = flight_history_df.withColumn('accumulated_delay', accumulated_delay_column('country_origin_name', 'delay_time')) \
                    .orderBy('country_origin_name', 'delay_time')
    accumulated_delay_df.show(10, False)

    print('> [Additional] Countries VIP:')
    vip_country_udf = udf(vip_country)
    vip_countries_df = take_arrive_country_df.withColumn('vip_country', vip_country_udf(col('country_code'))) \
                            .orderBy(col('country_code'))
    vip_countries_df.show(5, False)

    print('\n\nWriting the consolidated table <air_tribu.t_take_off_arrive_flight> in HIVE...')
    take_arrive_country_df.write.mode("overwrite").saveAsTable("air_tribu.t_take_off_arrive_flight")
    print('Writing the consolidated table <air_tribu.t_number_time_flight> in HIVE...')
    number_flight_time_df.write.mode("overwrite").saveAsTable("air_tribu.t_number_time_flight")
    print('Writing the consolidated table <air_tribu.t_delay_country_flight> in HIVE...')
    country_delay_flight_df.write.mode("overwrite").saveAsTable("air_tribu.t_delay_country_flight")
    print('[Additional] Writing countries VIP <air_tribu.t_vip_country> in HIVE...')
    vip_countries_df.repartition(10).filter(col('country_code') == 'PER').write.mode("overwrite").saveAsTable("air_tribu.t_vip_country")
    vip_countries_df.filter(col('country_code') == 'MEX').write.mode("append").saveAsTable("air_tribu.t_vip_country")
    print('Done.')

