"""Filter weather data to find the min values.

We use the 1900.csv dataset from the spark course on udacity.
The data has the format (weather_station_id, date[YYYYMMDD], type,,,?,):

ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,
"""


import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

SCHEMA = StructType([
    StructField("stationID", dataType=StringType(), nullable=True),
    StructField("date", dataType=IntegerType(), nullable=True),
    StructField("measure_type", dataType=StringType(), nullable=True),
    StructField("temperature", dataType=FloatType(), nullable=True),
])


def local_session(appname: str) -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master("local").appName(appname).getOrCreate()
    return spark


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="WeatherDataframes")

    # read the data into a dataframe
    df = spark.read.schema(SCHEMA).csv(filename)

    # get only minimum temperatures
    min_temps = df.filter(df.measure_type == "TMIN")

    # get min temp by station
    min_station_temps = min_temps.groupby("stationID").min("temperature")

    # convert to farenheit and only keep station and min temp
    min_station_temps = min_station_temps.withColumn(
        "temperature",
        functions.round(
            functions.col("min(temperature)") * 0.1 * (9.0/5.0) + 32,
            2
        )
    ).select("stationId", "temperature").sort("temperature")

    # show final results
    min_station_temps.show(min_station_temps.count())

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
