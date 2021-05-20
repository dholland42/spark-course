"""Count the number of times each word occurs in a book."""
import collections
from pprint import pprint

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
    sc = local_context(appname="WordCount")
    lines = sc.textFile(filename)

    # flatMap will return each word as it's own item,
    # instead of a list for each line
    words = lines.flatMap(lambda x: x.split())

    # then we can do the standard countByValue
    word_counts = words.countByValue()

    # only print ascii words
    ascii_words = filter(lambda x: x[0].encode('ascii', 'ignore'), word_counts.items())
    pprint(collections.OrderedDict(sorted(ascii_words, key=lambda x: x[1])))


if __name__ == "__main__":
    main()
