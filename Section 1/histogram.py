"""A simple script to count rating values from the movielens 100k dataset."""

import collections

import click
from pyspark import SparkConf, SparkContext


def local_context(appname: str) -> SparkContext:
    """Create a local spark context."""
    conf = SparkConf().setMaster("local").setAppName(appname)
    sc = SparkContext(conf=conf)
    return sc


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""
    sc = local_context(appname="RatingsHistogram")
    lines = sc.textFile(filename)
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()

    sortedResults = collections.OrderedDict(sorted(result.items()))
    for key, value in sortedResults.items():
        print(f"{key} {value}")


if __name__ == "__main__":
    main()
