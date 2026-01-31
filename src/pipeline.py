from pyspark.sql import SparkSession
from noaa.client import NOAAClient
from utils import date_ranges
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    split,
    when,
    unix_timestamp,
    to_date,
    to_timestamp,
    count,
    max,
    sum,
)


def normalize_and_validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Returns (good_df, bad_df)
    - required normalizations:
      1) expand concatenated strings to arrays
      2) timestamps -> epoch ms
      3) add ingestion timestamp
    - data validation:
      required fields + parseable date + non-negative value
    """

    # parse timestamp robustly (add patterns if needed

    df2 = (
        df.withColumn(
            "eventtime", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss")
        )  # TIMESTAMP
        .withColumn("eventdate", to_date(col("eventtime")))  # DATE
        .drop("date")
        .withColumn(
            "attributes_arr",
            when(col("attributes").isNull(), lit(None)).otherwise(
                split(col("attributes"), ",")
            ),
        )
        .drop("attributes")
        # epoch ms from parsed timestamp (NOT from raw string)
        .withColumn(
            "date_epoch_ms", (unix_timestamp(col("eventtime")) * 1000).cast(LongType())
        )
        .withColumn(
            "ingestion_ts",
            (unix_timestamp(current_timestamp()) * 1000).cast(LongType()),
        )
    )

    dfv = df2.withColumn(
        "bad_reason",
        when(col("eventtime").isNull(), lit("eventtime not parseable"))
        .when(col("date_epoch_ms").isNull(), lit("epoch ms not parseable"))
        .when(col("datatype").isNull(), lit("datatype is null"))
        .when(col("station").isNull(), lit("station is null"))
        .when(col("value").isNull(), lit("value is null"))
        .when(col("value") < 0, lit("value < 0"))
        .otherwise(lit(None)),
    )

    good = dfv.filter(col("bad_reason").isNull()).drop("bad_reason")
    bad = dfv.filter(col("bad_reason").isNotNull())

    return good, bad


class Pipeline:
    def __init__(
        self,
        spark: SparkSession,
        noaa: NOAAClient,
        catalog: str = "iceberg",
        db: str = "assigment",
        good_table_name: str = "good",
        bad_table_name: str = "bad",
        missing_metrics_teable_name: str = "missing_metrics",
        pipeline_state_name: str = "pipeline_state",
        chunkdays: int = 10,
    ):
        self.spark = spark
        self.noaa = noaa
        self.catalog = catalog
        self.db = db
        self.good_table_name = good_table_name
        self.bad_table_name = bad_table_name
        self.s3_base_location = f"s3a://{catalog}/{db}"
        self.chunkdays = chunkdays
        self.missing_metrics_teable_name = missing_metrics_teable_name
        self.pipeline_state_name = pipeline_state_name

        self.RAW_SCHEMA = StructType(
            [
                StructField("date", StringType(), True),
                StructField("datatype", StringType(), True),
                StructField("station", StringType(), True),
                StructField("attributes", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

    def init(self):
        """
        Ensure Iceberg tables exist with explicit locations.
        """
        spark = self.spark
        # print("Spark version:", spark.version)
        print(
            "iceberg catalog class:",
            spark.conf.get("spark.sql.catalog.iceberg", "MISSING"),
        )
        print(
            "iceberg catalog-impl:",
            spark.conf.get("spark.sql.catalog.iceberg.catalog-impl", "MISSING"),
        )
        print(
            "iceberg uri:", spark.conf.get("spark.sql.catalog.iceberg.uri", "MISSING")
        )

        # Namespace
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.db}
        """)

        # -------------------------
        # GOOD TABLE
        # -------------------------
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db}.{self.good_table_name} (
                eventtime TIMESTAMP,
                eventdate  DATE,
                datatype STRING,
                station STRING,
                attributes_arr ARRAY<STRING>,
                value INT,
                date_epoch_ms BIGINT,
                ingestion_ts BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self.good_table_name}'
            PARTITIONED BY (eventdate);
        """)

        # -------------------------
        # BAD TABLE
        # -------------------------
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db}.{self.bad_table_name} (
                enventtime STRING,
                datatype STRING,
                station STRING,
                attributes STRING,
                value INT,
                bad_reason STRING,
                ingestion_ts BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self.bad_table_name}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db}.{self.missing_metrics_teable_name} (
            station STRING,
            total_observations BIGINT,
            missing_observations BIGINT,
            missing_pct DOUBLE,
            updated_at_ms BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self.missing_metrics_teable_name}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db}.{self.pipeline_state_name} (
            pipeline_name STRING,
            last_ingestion_ts_ms BIGINT,
            updated_at_ms BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self.pipeline_state_name}'
        """)

        spark.sql(f"""
            MERGE INTO  {self.catalog}.{self.db}.{self.pipeline_state_name} t
            USING (
            SELECT
                '{self.pipeline_state_name}' AS pipeline_name,
                0L AS last_ingestion_ts_ms,
                cast(unix_millis(current_timestamp()) as bigint) AS updated_at_ms
            ) s
            ON t.pipeline_name = s.pipeline_name
            WHEN NOT MATCHED THEN INSERT *
        """)

    def ingest(self, startdate: str, totaldays: int):
        def push(buffer):
            df = self.spark.createDataFrame(chunk, schema=self.RAW_SCHEMA)
            print("start write xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", len(chunk))
            good, bad = normalize_and_validate(df)
            # Write to Iceberg (append)
            (
                good.writeTo(
                    f"{self.catalog}.{self.db}.{self.good_table_name}"
                ).overwritePartitions()
            )
            # bad.writeTo(f"{self.catalog}.{self.db}.{self.bad_table_name}").append()
            print("done writing XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

        for startday, endday in date_ranges(startdate, totaldays, self.chunkdays):
            while True:
                chunk = []
                for batch in self.noaa.fetch_date_range(startday, endday):
                    chunk += batch
                    print(
                        f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx {startday, endday} reading {len(chunk)}"
                    )
                push(chunk)
                break

    def transform(self):
        def get_last_watermark() -> int:
            row = (
                self.spark.table(f"{self.catalog}.{self.db}.{self.pipeline_state_name}")
                .filter(col("pipeline_name") == "{self.pipeline_state_name}")
                .select("last_ingestion_ts_ms")
                .collect()
            )
            return int(row[0]["last_ingestion_ts_ms"]) if row else 0

        def set_last_watermark(new_wm: int) -> None:
            now_ms = int(
                self.spark.sql(
                    "SELECT cast(unix_millis(current_timestamp()) as bigint) AS x"
                ).collect()[0]["x"]
            )
            self.spark.sql(f"""
                MERGE INTO {self.catalog}.{self.db}.{self.pipeline_state_name} t
                USING (SELECT
                        '{self.pipeline_state_name}' AS pipeline_name,
                        {new_wm} AS last_ingestion_ts_ms,
                        {now_ms} AS updated_at_ms
                    ) s
                ON t.pipeline_name = s.pipeline_name
                WHEN MATCHED THEN UPDATE SET
                last_ingestion_ts_ms = s.last_ingestion_ts_ms,
                updated_at_ms = s.updated_at_ms
                WHEN NOT MATCHED THEN INSERT *
            """)

        last_wm = get_last_watermark()

        src = (
            self.spark.table(f"{self.catalog}.{self.db}.{self.good_table_name}")
            .filter(col("ingestion_ts") > lit(last_wm))
            .select("station", "value", "ingestion_ts")
        )

        # If there is no new data, exit cleanly
        if src.limit(1).count() == 0:
            return

        # Aggregate only the new batch
        batch_agg = src.groupBy("station").agg(
            count(lit(1)).cast("bigint").alias("batch_total"),
            sum(when(col("value") == lit(99999), lit(1)).otherwise(lit(0)))
            .cast("bigint")
            .alias("batch_missing"),
            max("ingestion_ts").cast("bigint").alias("batch_max_ingestion_ts_ms"),
        )

        # Create a temp view for MERGE
        batch_agg.createOrReplaceTempView("batch_station_missing")

        # MERGE: add counts incrementally, recompute pct
        self.spark.sql(f"""
            MERGE INTO {self.catalog}.{self.db}.{self.missing_metrics_teable_name} t
            USING (
            SELECT
                station,
                batch_total,
                batch_missing,
                batch_max_ingestion_ts_ms
            FROM batch_station_missing
            ) s
            ON t.station = s.station
            WHEN MATCHED THEN UPDATE SET
            total_observations   = t.total_observations + s.batch_total,
            missing_observations = t.missing_observations + s.batch_missing,
            missing_pct = CASE
                WHEN (t.total_observations + s.batch_total) = 0 THEN 0.0
                ELSE (t.missing_observations + s.batch_missing) * 100.0 / (t.total_observations + s.batch_total)
            END,
            updated_at_ms = cast(unix_millis(current_timestamp()) as bigint)
            WHEN NOT MATCHED THEN INSERT (
            station,
            total_observations,
            missing_observations,
            missing_pct,
            updated_at_ms
            ) VALUES (
            s.station,
            s.batch_total,
            s.batch_missing,
            CASE
                WHEN s.batch_total = 0 THEN 0.0
                ELSE s.batch_missing * 100.0 / s.batch_total
            END,
            cast(unix_millis(current_timestamp()) as bigint)
            )
        """)

        # Advance watermark to max ingestion_ts_ms seen in this run
        new_wm = batch_agg.agg(max("batch_max_ingestion_ts_ms").alias("m")).collect()[
            0
        ]["m"]
        set_last_watermark(int(new_wm))
