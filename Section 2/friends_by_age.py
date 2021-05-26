"""Count friends in a network and group by age.

We use the fakefriends.csv dataset from the spark course on udacity.
The format of the csv is (index, name, age, num_friends):

    0,Will,33,385
    1,Jean-Luc,26,2
    2,Hugh,55,221
    3,Deanna,40,465
    4,Quark,68,21
"""

from typing import Tuple
from pprint import pprint

import click
from pyspark import SparkConf, SparkContext


def local_context(appname: str) -> SparkContext:
    """Create a local spark context."""
    conf = SparkConf().setMaster("local[*]").setAppName(appname)
    sc = SparkContext(conf=conf)
    return sc


def parse_line(line: str) -> Tuple[int, int]:
    """Parse age and number of friends from a single line."""
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""
    sc = local_context(appname="FriendsByAge")
    lines = sc.textFile(filename)
    parsed = lines.map(parse_line)
    total_friends_by_age = parsed.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_friends_by_age = total_friends_by_age.mapValues(lambda x: x[0] / x[1])
    results = average_friends_by_age.collect()
    pprint(sorted(results, key=lambda row: row[0]))


if __name__ == "__main__":
    main()
