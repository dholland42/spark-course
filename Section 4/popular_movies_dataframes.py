"""A simple script to count rating values from the movielens 100k dataset.

    Data has the format (tab separated):
    (user_id    movie_id    rating  timestamp)
"""

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
)

SCHEMA = StructType([
    StructField("user_id", dataType=IntegerType(), nullable=True),
    StructField("movie_id", dataType=IntegerType(), nullable=True),
    StructField("rating", dataType=IntegerType(), nullable=True),
    StructField("timestamp", dataType=LongType(), nullable=True),
])


def local_session(appname: str, cpus: str = "*") -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master(f"local[{cpus}]")
    spark = spark.appName(appname)
    return spark.getOrCreate()


def load_movienames(filename: str) -> dict:
    with open(filename, mode="r", encoding="ISO-8859-1") as f:
        movienames = dict()
        for line in f:
            fields = line.split("|")
            movienames[int(fields[0])] = fields[1]
    return movienames


@click.command()
@click.option("--filename", "-f", required=True)
@click.option("--movie-names", "-m", default="")
def main(filename: str, movie_names: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="PopularMovies")

    # read in the tab separated data
    df = spark.read.option("sep", "\t").schema(SCHEMA).csv(filename)

    # get most rated movies
    popular_movies = df.groupBy("movie_id").count().orderBy(functions.desc("count"))

    # movie id 50 is most popular ... but what the
    # heck is movie 50? Let's add the names
    # we'll use this opportuinity to demonstrate how
    # to use broadcast variables
    if movie_names:
        names_dict = spark.sparkContext.broadcast(load_movienames(movie_names))

        # create a user-defined function (UDF)
        def lookup_name(movie_id):
            return names_dict.value[movie_id]
        lookup_name_udf = functions.udf(lookup_name)

        # use our brand new udf to add a `movie_title` column
        movies_with_names = popular_movies.withColumn("movie_title", lookup_name_udf(functions.col("movie_id")))

        # sort and show results
        movies_with_names = movies_with_names.orderBy(functions.desc("count"))
        movies_with_names.show(10, truncate=False)

    else:
        # show top 10 most rated movies
        popular_movies.show(10)

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
