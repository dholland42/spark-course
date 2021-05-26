"""Count the number of times each word occurs in some text.

    NOTE: This is a pretty poor use case for spark sql. While it
    is technically possible to do, this is an unstructured data
    problem and is more well suited to the rdd api.
"""

import click
from pyspark.sql import SparkSession, functions


def local_session(appname: str) -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master("local").appName(appname).getOrCreate()
    return spark


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="WordCountDataframes")

    # here we'll read in the book text so that each line
    # is a row in our dataframe
    df = spark.read.text(filename)

    # we can use functions.explode to act like rdd.flatmap
    # this will explode each line so that now each row in
    # our dataframe is a single word in a column named `word`
    words = df.select(functions.explode(functions.split(df.value, "\\W+")).alias("word"))

    # remove blanks
    words.filter(words.word != "")

    # lowercase the words
    lowercase = words.select(functions.lower(words.word).alias("word"))

    # perform the count and displlay the results, sorted
    # by the number of times the word appears in the book
    word_counts = lowercase.groupBy("word").count()
    word_counts_sorted = word_counts.sort("count")
    word_counts_sorted.show(word_counts_sorted.count())

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
