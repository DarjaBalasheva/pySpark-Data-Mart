import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, DoubleType, StringType, IntegerType, StructType

airlines_schema = StructType([
    StructField('iata_code', StringType(), True),
    StructField('airline', StringType(), True),
])


flights_schema = StructType([
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('day', IntegerType(), True),
    StructField('day_of_week', IntegerType(), True),
    StructField('airline', StringType(), True),
    StructField('flight_number', IntegerType(), True),
    StructField('tail_number', StringType(), True),
    StructField('origin_airport', StringType(), True),
    StructField('destination_airport', StringType(), True),
    StructField('scheduled_departure', IntegerType(), True),
    StructField('departure_time', DoubleType(), True),
    StructField('departure_delay', DoubleType(), True),
    StructField('taxi_out', DoubleType(), True),
    StructField('wheels_off', DoubleType(), True),
    StructField('scheduled_time', IntegerType(), True),
    StructField('elapsed_time', DoubleType(), True),
    StructField('air_time', DoubleType(), True),
    StructField('distance', IntegerType(), True),
    StructField('wheels_on', DoubleType(), True),
    StructField('taxi_in', DoubleType(), True),
    StructField('scheduled_arrival', IntegerType(), True),
    StructField('arrival_time', DoubleType(), True),
    StructField('arrival_delay', DoubleType(), True),
    StructField('diverted', IntegerType(), True),
    StructField('cancelled', IntegerType(), True),
    StructField('cancellation_reason', StringType(), True),
    StructField('air_system_delay', DoubleType(), True),
    StructField('security_delay', DoubleType(), True),
    StructField('airline_delay', DoubleType(), True),
    StructField('late_aircraft_delay', DoubleType(), True),
    StructField('weather_delay', DoubleType(), True),
])


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    # Extract dataset
    flights_dim = (spark.read
                   .option("header", "true")
                   .schema(flights_schema)
                   .parquet(flights_path))

    airlines_dim = (spark.read
                    .option("header", "true")
                    .schema(airlines_schema)
                    .parquet(airlines_path)).withColumnRenamed('airline', 'airline_name')
    
    # Transform dataset to target datamart
    datamart = (flights_dim
    .groupby(flights_dim['airline'])
    .agg(F.sum(F.when(flights_dim['diverted'] == 1, 1)
               .otherwise(0))
         .alias('diverted_count'),
         F.sum(F.when(flights_dim['cancelled'] == 1, 1)
               .otherwise(0))
         .alias('cancelled_count'),
         F.sum(F.when(flights_dim['cancelled'] == 1, 0)
               .when(flights_dim['diverted'] == 1, 0)
               .otherwise(1))
         .alias('correct_count'),
         F.avg(flights_dim['distance']).alias('avg_distance'),
         F.avg(flights_dim['air_time']).alias('avg_air_time'),
         F.sum(
             F.when(flights_dim['cancellation_reason'] == 'A', 1).otherwise(0)
         ).alias('airline_issue_count'),
         F.sum(
             F.when(flights_dim['cancellation_reason'] == 'B', 1).otherwise(0)
         ).alias('weather_issue_count'),
         F.sum(
             F.when(flights_dim['cancellation_reason'] == 'C', 1).otherwise(0)
         ).alias('nas_issue_count'),
         F.sum(
             F.when(flights_dim['cancellation_reason'] == 'D', 1).otherwise(0)
         ).alias('security_issue_count')
         )
    .join(other=airlines_dim,
          on=flights_dim['airline'] == airlines_dim['iata_code'],
          how='inner')
    .select(
        F.col('airline_name').alias('AIRLINE_NAME'),
        F.col('correct_count'),
        F.col('diverted_count'),
        F.col('cancelled_count'),
        F.col('avg_distance'),
        F.col('avg_air_time'),
        F.col('airline_issue_count'),
        F.col('weather_issue_count'),
        F.col('nas_issue_count'),
        F.col('security_issue_count')
    )
    )

    airlines_dim.show(truncate=True, n=100)
    # Save result to parquet
    datamart.write.mode("overwrite").format("parquet").save(result_path)




def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='data/airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result/PySparkJob4', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
