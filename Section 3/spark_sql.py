"""Analyze the friends dataset using spark sql.

We use the fakefriends.csv dataset from the spark course on udacity.
The format of the csv is (index, name, age, num_friends):

    0,Will,33,385
    1,Jean-Luc,26,2
    2,Hugh,55,221
    3,Deanna,40,465
    4,Quark,68,21
"""

from pprint import pprint

import click
from pyspark.sql import SparkSession, Row


def local_session(appname: str) -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master("local").appName(appname).getOrCreate()
    return spark


def parse_line(line: str) -> Row:
    """Parse a single line into a named, typed row."""
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1]),
        age=int(fields[2]),
        nfriends=int(fields[3]),
    )


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="FriendsByAge")

    # but you can access a context from the session, and read a text file
    # in exactly the same way as before
    lines = spark.sparkContext.textFile(filename)

    # parse into rows so we can create a dataframe
    parsed = lines.map(parse_line)
    schema_people = spark.createDataFrame(parsed).cache()

    # create a temp view so we can run sql-like queries against it
    schema_people.createOrReplaceTempView("people")

    # show only the teenagers
    teens = spark.sql("""select * from people where age >= 13 and age < 20""")
    pprint(teens.collect())

    # show total number of observations by age
    schema_people.groupBy("age").count().orderBy("age").show()

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
