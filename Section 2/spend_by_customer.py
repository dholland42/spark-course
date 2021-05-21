"""First exercise in the course - calculate spend by customer.

This script takes the data in `customer-orders.csv` and computes
the total spend by customer. The data in the csv is of the format:

    (customer_id, item_id, amount_spend)
"""
from typing import Tuple
from pprint import pprint

import click
from pyspark import SparkConf, SparkContext


def local_context(appname: str) -> SparkContext:
    """Create a local spark context."""
    conf = SparkConf().setMaster("local").setAppName(appname)
    sc = SparkContext(conf=conf)
    return sc


def parse_line(line: str) -> Tuple[int, int, float]:
    """Parse a line from the csv file into numerical data."""
    customer_id, item_id, spend = line.split(",")
    return int(customer_id), int(item_id), float(spend)


@click.command()
@click.option("--filename", "-f", required=True)
def main(filename: str) -> None:
    """Main entry point and logic for the program."""
    sc = local_context(appname="WordCount")
    # read the data
    lines = sc.textFile(filename)
    # parse into numerical data
    data = lines.map(parse_line)
    # we only care about the customer_id and spend
    data = data.map(lambda x: (x[0], x[2]))
    # compute total spend by customer
    data = data.reduceByKey(lambda current, new: current + new)
    # display results
    pprint(data.collect())


if __name__ == "__main__":
    main()