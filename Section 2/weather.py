"""Filter weather data to find the min values.

We use the 1900.csv dataset from the spark course on udacity.
The data has the format (weather_station_id, date[YYYYMMDD], type,,,?,):

ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,
"""

from typing import Tuple
from pprint import pprint

import click
from pyspark import SparkConf, SparkContext


def local_context(appname: str) -> SparkContext:
    """Create a local spark context."""
    conf = SparkConf().setMaster("local").setAppName(appname)
    sc = SparkContext(conf=conf)
    return sc


def parse_line(line: str) -> Tuple[str, str, float]:
    """Parse station id, entry type and temperature from a row."""
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temp = float(fields[3]) / 10 * (9 / 5) + 32  # convert to farenheit
    return (station_id, entry_type, temp)


@click.command()
@click.option("--filename", "-f", required=True)
@click.option("--mode", "-m", type=click.Choice(["MIN", "MAX"], case_sensitive=False), default="min")
def main(filename: str, mode: str) -> None:
    """Main entry point and logic for the program."""
    sc = local_context(appname="Weather1800")
    lines = sc.textFile(filename)
    parsed = lines.map(parse_line)
    entry_type = f"T{mode}"

    # filter to only min/max temps for each day
    temps = parsed.filter(lambda x: entry_type in x[1])

    # remove the type field
    temps = temps.map(lambda x: (x[0], x[2]))

    # get the min/max temp by station
    op = min if mode == "MIN" else max
    temps = temps.reduceByKey(lambda x, y: op(x, y))

    # format and print results
    results = temps.collect()
    pprint(sorted(map(lambda x: (x[0], f"{x[1]:.2f}F"), results), key=lambda x: x[-1]))


if __name__ == "__main__":
    main()
