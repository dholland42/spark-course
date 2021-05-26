"""Calculate spend by customer.

This script takes the data in `customer-orders.csv` and computes
the total spend by customer using dataframes.

The data in the csv is of the format:

    (customer_id, item_id, amount_spend)
"""


import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)

SCHEMA = StructType([
    StructField("customer_id", dataType=IntegerType(), nullable=True),
    StructField("item_id", dataType=IntegerType(), nullable=True),
    StructField("spend", dataType=FloatType(), nullable=True),
])


def local_session(appname: str, cpus: str = "*") -> SparkSession:
    """Create a local spark context."""
    spark = SparkSession.builder.master(f"local[{cpus}]")
    spark = spark.appName(appname)
    return spark.getOrCreate()


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""

    # spark sql uses a session, not a context
    spark = local_session(appname="WeatherDataframes")

    # read the data into a dataframe
    df = spark.read.schema(SCHEMA).csv(filename)

    # group by customer id and calculate average spend
    grouped = df.groupBy("customer_id")

    # here we use agg and func.sum instead of just .sum
    # so that we can round the result and appropriately
    # alias the column
    spend_by_customer = grouped.agg(
        functions.round(
            functions.sum("spend"),
            2
        ).alias("total_spend")
    )

    # sort and show the results
    spend_by_customer.sort("total_spend").show(spend_by_customer.count())

    # don't forget to stop the session!
    spark.stop()


if __name__ == "__main__":
    main()
