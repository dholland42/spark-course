"""Count the number of times each word occurs in some text."""
import re
import collections
from pprint import pprint
from typing import List

import click
from pyspark import SparkConf, SparkContext


def local_context(appname: str) -> SparkContext:
    """Create a local spark context."""
    conf = SparkConf().setMaster("local[*]").setAppName(appname)
    sc = SparkContext(conf=conf)
    return sc


WORDPATTERN = re.compile(r"\W+", re.UNICODE)


def normalize_words(text: str) -> List[str]:
    """Normalize an input text by lowercasing and splitting out only words."""
    return WORDPATTERN.split(text.lower())


@click.command()
@click.option("--filename", "-f", required=True)
@click.option("--clean", "-c", is_flag=True, default=False)
@click.option("--sort", "-s", is_flag=True, default=False)
def main(filename: str, clean: bool, sort: bool) -> None:
    """Main entry point and logic for the program."""
    sc = local_context(appname="WordCount")
    lines = sc.textFile(filename)

    # flatMap will return each word as it's own item,
    # instead of a list for each line
    if clean:
        # kill punctuation and split
        words = lines.flatMap(normalize_words)
    else:
        words = lines.flatMap(lambda x: x.split())

    if sort:
        # do what CountByValue does, but don't compute - just create a new rdd
        word_counts_sorted = words.map(lambda word: (word, 1)).reduceByKey(lambda current, new: current + new)
        # then flip keys and values around so we can sort
        word_counts_sorted = word_counts_sorted.map(lambda wordcount: (wordcount[1], wordcount[0])).sortByKey()
        # compute and pull into python
        word_counts = word_counts_sorted.map(lambda countword: (countword[1], countword[0])).collect()
        # cast to a dict for downstream processing
        word_counts = dict(word_counts)
    else:
        # then we can do the standard countByValue
        word_counts = words.countByValue()

    # only print ascii words
    ascii_words = filter(lambda x: x[0].encode('ascii', 'ignore'), word_counts.items())
    pprint(collections.OrderedDict(ascii_words))


if __name__ == "__main__":
    main()
