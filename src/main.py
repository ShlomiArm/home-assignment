from noaa.client import NOAAClient
from config import NOAAConfig, PipelineConfig
from pipeline import Pipeline
from pyspark.sql import SparkSession


def init(pipeline: Pipeline):
    """
    This function is used to initialise the datalake schema before running the pipelines.
    :return:
    """
    pipeline.init()


def ingest(pipeline: Pipeline, startdate: str, totaldays: int):
    """
    This function is used to ingest data from the usgs earthquake api source to an iceberg table.
    :return:
    """
    pipeline.ingest(startdate, totaldays)


def transform(
    pipeline: Pipeline,
):
    """
    This function is used to transform the data from the iceberg table populated by the ingest method.
    :return:
    """
    pipeline.transform()


def maintain(pipeline: Pipeline):
    """
    This function is used to maintain all data in the iceberg schema.
    :return:
    """
    pipeline.maintain()


def main():
    spark = spark = SparkSession.builder.appName("NOAA Pipeline").getOrCreate()

    p = Pipeline(spark, NOAAClient(NOAAConfig()), PipelineConfig())
    init(p)

    ingest(p, startdate="2010-05-01", totaldays=30)
    transform(p)
    # maintain(p)


if __name__ == "__main__":
    main()
