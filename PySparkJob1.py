import argparse
import tempfile

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import os
from pathlib import Path
from pyspark.sql.types import (StructType,
                               StructField,
                               StringType,
                               DoubleType,
                               IntegerType)

# Dataset schemas
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
    StructField('departure_delay', IntegerType(), True),
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
    Main process of task.

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
                .where(airlines_dim['tail_number'].isNotNull())
                .groupBy(airlines_dim['tail_number'])
                .agg(F.count(airlines_dim['tail_number']).alias('count'))
                .select(F.col('tail_number').alias("TAIL_NUMBER"),
                        F.col('count'))
                .orderBy(F.col('count').desc())
                .limit(10))

    datamart.show(truncate=True)

    schema = StructType([
        StructField("TAIL_NUMBER", StringType(), True),
        StructField("count", IntegerType(), True)
    ])
    # Save result to parquet
    datamart.write.mode("overwrite").format("parquet").option("schema", schema).save(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Create SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='data/flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result/PySparkJob1', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
