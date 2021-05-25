"""Count friends in a network and group by age using dataframes.

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
    spark = local_session(appname="FriendsByAge")

    # read the csv file with a header directly into a dataframe
    #
    # set up our reading options so that we handle the header row
    # and infer the schema from the top of the dataset
    reader = spark.read.option("header", "true").option("inferSchema", "true")
    # actually read the data into a dataframe
    parsed = reader.csv(filename)

    # count friends by age and format for display
    #
    # group the data by age
    grouped = parsed.groupBy(parsed.age)
    # aggregate by averaging the `friends` column
    # then truncate to 2 decimal places and rename the resulting
    # column `avg_friends`
    friends_by_age = grouped.agg(
        functions.round(
            functions.avg("friends"),
            2
        ).alias("avg_friends")
    )
    # sort the final results by age
    friends_by_age = friends_by_age.sort("age")

    # here we show the _entire_ resulting dataframe since we know if it small
    # you typically will not want to do this in real life scenarios as this
    # will pull the entire dataframe onto the controller
    friends_by_age.show(friends_by_age.count(), False)


if __name__ == "__main__":
    main()
