from dataclasses import dataclass
from pyspark.sql import SparkSession
from typing import Dict, List
import os

@dataclass
class NOAAConfig:
    base_url: str = "http://www.ncei.noaa.gov/cdo-web/api/v2/data"
    timeout_s: int = 60
    default_limit: int = 1000
    sleep_s: float = 0.1
    dataset_id: str = "PRECIP_15"
    token: str = os.getenv("NOAA_TOKEN")

    

class SparkConfig:
    def __init__(self):
        self.packages: List[str] = [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]

        self.conf: Dict[str, str] = {
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            "spark.sql.catalog.iceberg":
                "org.apache.iceberg.spark.SparkCatalog",

            "spark.sql.catalog.iceberg.catalog-impl":
                "org.apache.iceberg.rest.RESTCatalog",

            "spark.sql.catalog.iceberg.uri":
                "http://localhost:8183",

            "spark.sql.catalog.iceberg.io-impl":
                "org.apache.iceberg.hadoop.HadoopFileIO",

            "spark.sql.catalog.iceberg.warehouse":
                "s3a://iceberg/",

            # S3 / MinIO
            "spark.hadoop.fs.s3a.endpoint":
                "http://localhost:9000",

            "spark.hadoop.fs.s3a.path.style.access":
                "true",

            "spark.hadoop.fs.s3a.access.key":
                "minio",

            "spark.hadoop.fs.s3a.secret.key":
                "minio123",

            "spark.hadoop.fs.s3a.connection.ssl.enabled":
                "false",

            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",

            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }

    def apply(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        # packages
        builder = builder.config(
            "spark.jars.packages",
            ",".join(self.packages)
        )

        # spark confs
        for k, v in self.conf.items():
            builder = builder.config(k, v)

        return builder
        
    