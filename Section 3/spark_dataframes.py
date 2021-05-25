"""Analyze the friends dataset using spark sql dataframes only.

We use the fakefriends-header.csv dataset from the spark course on udacity.
The format of the csv is (index, name, age, num_friends):

    userID,name,age,friends
    0,Will,33,385
    1,Jean-Luc,26,2
    2,Hugh,55,221
    3,Deanna,40,465
    4,Quark,68,21
"""

import click
from pyspark.sql import SparkSession


def local_session(appname: str) -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master("local").appName(appname).getOrCreate()
    return spark


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="FriendsByAge")

    # read the csv file with a header directly into a dataframe
    parsed = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)
    print("Inferred schema:")
    parsed.printSchema()

    # show the name column
    parsed.select("name").show()

    # show people under 21
    parsed.filter(parsed.age < 21).show()

    # Count of people by age
    parsed.groupBy("age").count().show()

    # Show everyone in 10 years
    parsed.select(parsed.name, parsed.age + 10).show()

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
