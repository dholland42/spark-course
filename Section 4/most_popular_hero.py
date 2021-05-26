"""Find the most popular Marvel super hero.

We have 2 input files:
    - marvel-graph.txt
    - marvel-names.txt

and they have the following formats:

[marvel-graph.txt] - every row is a space separated list of hero ids
id id id id id id id ...

[marvel-names.txt]
id name
"""

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

SCHEMA = StructType([
    StructField("id", dataType=IntegerType(), nullable=True),
    StructField("name", dataType=StringType(), nullable=True),
])


def local_session(appname: str, cpus: str = "*") -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master(f"local[{cpus}]")
    spark = spark.appName(appname)
    return spark.getOrCreate()


@click.command()
@click.option("--filename", "-f", required=True)
@click.option("--names", "-n", default=True)
def main(filename: str, names: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="SuperHeros")

    names = spark.read.schema(SCHEMA).option("sep", " ").csv(names)

    lines = spark.read.text(filename)
    connections = lines.withColumn(
        "id",
        functions.split(functions.col("value"), " ")[0]
    ).withColumn(
        "connections",
        functions.size(functions.split(functions.col("value"), " ")) - 1
    ).groupBy("id").agg(functions.sum("connections").alias("connections"))

    most_popular = connections.sort(functions.col("connections").desc()).first()
    most_popular_name = names.filter(functions.col("id") == most_popular[0]).select("name").first()

    print(f"{most_popular_name[0]} is the most popular hero with {most_popular[1]} connections!")

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
