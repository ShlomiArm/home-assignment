from noaa.client import NOAAClient
from config import NOAAConfig, SparkConfig
from pipeline import Pipeline
from pyspark.sql import SparkSession


"""export NOAA_TOKEN=jVHziZKyvmrtqylHvUyOQfSmnNMltSMi && spark-submit   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions   --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog   --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.rest.RESTCatalog   --conf spark.sql.catalog.iceberg.uri=http://localhost:8183   --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.hadoop.HadoopFileIO   --conf spark.sql.catalog.iceberg.warehouse=s3a://iceberg/   --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000   --conf spark.hadoop.fs.s3a.path.style.access=true   --conf spark.hadoop.fs.s3a.access.key=minio   --conf spark.hadoop.fs.s3a.secret.key=minio123   --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false   --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem   --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider   src/main.py"""


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


def maintain():
    """
    This function is used to maintain all data in the iceberg schema.
    :return:
    """
    pass


def main():
    noaa = NOAAClient(NOAAConfig())

    spark_builder = SparkSession.builder.appName("NOAA Pipeline")

    spark = SparkConfig().apply(spark_builder).getOrCreate()

    p = Pipeline(spark, noaa)
    init(p)

    ingest(p, startdate="2010-05-01", totaldays=30)
    transform(p)
    # maintain()


if __name__ == "__main__":
    main()
