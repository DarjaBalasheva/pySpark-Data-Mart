import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

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
    StructField('departure_time', IntegerType(), True),
    StructField('departure_delay', DoubleType(), True),
    StructField('taxi_out', IntegerType(), True),
    StructField('wheels_off', IntegerType(), True),
    StructField('scheduled_time', IntegerType(), True),
    StructField('elapsed_time', IntegerType(), True),
    StructField('air_time', DoubleType(), True),
    StructField('distance', IntegerType(), True),
    StructField('wheels_on', IntegerType(), True),
    StructField('taxi_in', IntegerType(), True),
    StructField('scheduled_arrival', IntegerType(), True),
    StructField('arrival_time', IntegerType(), True),
    StructField('arrival_delay', IntegerType(), True),
    StructField('diverted', IntegerType(), True),
    StructField('cancelled', IntegerType(), True),
    StructField('cancellation_reason', StringType(), True),
    StructField('air_system_delay', IntegerType(), True),
    StructField('security_delay', IntegerType(), True),
    StructField('airline_delay', IntegerType(), True),
    StructField('late_aircraft_delay', IntegerType(), True),
    StructField('weather_delay', IntegerType(), True),
])


def process(spark, flights_path, result_path):
    """

    :param spark: SparkSession
    :param flights_path: path to flights dataset
    :param result_path: path to save results
    """

    data_path = flights_path

    # Extract dataset
    airlines_dim = (spark.read
                    .option("header", "true")
                    .schema(flights_schema)
                    .parquet(data_path))
    # Transform dataset to target datamart
    datamart = (airlines_dim
                .where(airlines_dim['origin_airport'].isNotNull())
                .groupBy(airlines_dim['origin_airport'])
                .agg(F.avg(airlines_dim['departure_delay']).alias('avg_delay'),
                     F.min(airlines_dim['departure_delay']).alias('min_delay'),
                     F.max(airlines_dim['departure_delay']).alias('max_delay'),
                     F.corr("departure_delay", "day_of_week").alias("corr_delay2day_of_week"))
                .select(F.col('origin_airport').alias("ORIGIN_AIRPORT"),
                        F.col('avg_delay'),
                        F.col('min_delay'),
                        F.col('max_delay'),
                        F.col('corr_delay2day_of_week'))
                .filter(F.col('max_delay') > 1000))

    datamart.show(truncate=True)
    # Save result to parquet
    datamart.write.mode("overwrite").format("parquet").save(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result/PySparkJob3', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
