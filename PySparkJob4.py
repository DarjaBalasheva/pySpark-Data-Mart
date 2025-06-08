import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

airlines_schema = StructType([
    StructField('iata_code', StringType(), True),
    StructField('airline', StringType(), True),
])

airport_schema = StructType([
    StructField('iata_code', StringType(), True),
    StructField('airport', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('country', StringType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('longitude', DoubleType(), True),
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


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    flights_dim = (spark.read
                   .option("header", "true")
                   .schema(flights_schema)
                   .parquet(flights_path))
    #  Extract dataset
    airlines_dim = (spark.read
                    .option("header", "true")
                    .schema(airlines_schema)
                    .parquet(airlines_path)).withColumnRenamed('airline', 'airline_name')

    origin_airports_dim = ((spark.read
                            .option("header", "true")
                            .schema(airport_schema)
                            .parquet(airports_path))
                           .withColumnRenamed('country', 'origin_country')
                           .withColumnRenamed('airport', 'origin_airport_name')
                           .withColumnRenamed('longitude', 'origin_longitude')
                           .withColumnRenamed('latitude', 'origin_latitude')
                           .withColumnRenamed('iata_code', 'origin_iata')
                           .withColumnRenamed('city', 'origin_city')
                           .withColumnRenamed('state', 'origin_state'))

    destination_airport_dim = ((spark.read
                                .option("header", "true")
                                .schema(airport_schema)
                                .parquet(airports_path))
                               .withColumnRenamed('country', 'destination_country')
                               .withColumnRenamed('airport', 'destination_airport_name')
                               .withColumnRenamed('latitude', 'destination_latitude')
                               .withColumnRenamed('longitude', 'destination_longitude')
                               .withColumnRenamed('iata_code', 'destination_iata')
                               .withColumnRenamed('city', 'destination_city')
                               .withColumnRenamed('state', 'destination_state'))
    # Transform dataset to target datamart
    datamart = (flights_dim
    .join(other=origin_airports_dim,
          on=flights_dim['origin_airport'] == origin_airports_dim['origin_iata'],
          how='inner')
    .join(other=destination_airport_dim,
          on=flights_dim['destination_airport'] == destination_airport_dim['destination_iata'],
          how='inner')
    .join(other=airlines_dim,
          on=flights_dim['airline'] == airlines_dim['iata_code'],
          how='inner')
    .select(
        F.col('airline_name').alias('AIRLINE_NAME'),
        F.col('tail_number').alias('TAIL_NUMBER'),
        F.col('origin_country').alias('ORIGIN_COUNTRY'),
        F.col('origin_airport_name').alias('ORIGIN_AIRPORT_NAME'),
        F.col('origin_latitude').alias('ORIGIN_LATITUDE'),
        F.col('origin_longitude').alias('ORIGIN_LONGITUDE'),
        F.col('destination_country').alias('DESTINATION_COUNTRY'),
        F.col('destination_airport_name').alias('DESTINATION_AIRPORT_NAME'),
        F.col('destination_latitude').alias('DESTINATION_LATITUDE'),
        F.col('destination_longitude').alias('DESTINATION_LONGITUDE'),
    ))
    datamart.show(truncate=True, n=100)
    # Save result to parquet
    datamart.write.mode("overwrite").format("parquet").save(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='data/airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='data/airports.parquet',
                        help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result/PySparkJob4', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
