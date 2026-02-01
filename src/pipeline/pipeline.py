from pyspark.sql import SparkSession, Row
from noaa import NOAAClient
from config import PipelineConfig, NOAAConfig
from pyspark.sql import DataFrame
import os
from typing import Tuple, Iterator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.storagelevel import StorageLevel
import sys
import socket
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    split,
    when,
    unix_timestamp,
    to_timestamp,
    count,
    max,
    sum,
)

pid = os.getpid()
host = socket.gethostname()


def normalize_and_validate(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
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
        .withColumn(
            "attributes_arr",
            when(col("attributes").isNull(), lit(None)).otherwise(
                split(col("attributes"), ",")
            ),
        )
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

    good = (
        dfv.select(
            "eventtime",
            "datatype",
            "station",
            "attributes_arr",
            "value",
            "date_epoch_ms",
            "ingestion_ts",
            "bad_reason",
        )
        .filter(col("bad_reason").isNull())
        .drop("bad_reason")
    )
    bad = dfv.select(
        "date",
        "datatype",
        "station",
        "attributes",
        "value",
        "bad_reason",
        "ingestion_ts",
    ).filter(col("bad_reason").isNotNull())

    return good, bad


class Pipeline:
    def __init__(self, spark: SparkSession, noaa: NOAAClient, cfg: PipelineConfig):
        self._spark = spark
        self._noaa = noaa
        self._cfg = cfg
        self.s3_base_location = f"s3a://{self._cfg.catalog}/{self._cfg.db}"

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
        spark = self._spark
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
            CREATE SCHEMA IF NOT EXISTS {self._cfg.catalog}.{self._cfg.db}
        """)

        # -------------------------
        # GOOD TABLE
        # -------------------------
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._cfg.catalog}.{self._cfg.db}.{self._cfg.good_table_name} (
                eventtime TIMESTAMP,
                datatype STRING,
                station STRING,
                attributes_arr ARRAY<STRING>,
                value INT,
                date_epoch_ms BIGINT,
                ingestion_ts BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self._cfg.good_table_name}'
            PARTITIONED BY (days(eventtime))
        """)

        # -------------------------
        # BAD TABLE
        # -------------------------
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._cfg.catalog}.{self._cfg.db}.{self._cfg.bad_table_name} (
                date STRING,
                datatype STRING,
                station STRING,
                attributes STRING,
                value INT,
                bad_reason STRING,
                ingestion_ts BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self._cfg.bad_table_name}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._cfg.catalog}.{self._cfg.db}.{self._cfg.missing_metrics_table_name} (
            station STRING,
            total_observations BIGINT,
            missing_observations BIGINT,
            missing_pct DOUBLE,
            updated_at_ms BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self._cfg.missing_metrics_table_name}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._cfg.catalog}.{self._cfg.db}.{self._cfg.pipeline_state_name} (
            pipeline_name STRING,
            last_ingestion_ts_ms BIGINT,
            updated_at_ms BIGINT
            )
            USING iceberg
            LOCATION '{self.s3_base_location}/{self._cfg.pipeline_state_name}'
        """)

        spark.sql(f"""
            MERGE INTO  {self._cfg.catalog}.{self._cfg.db}.{self._cfg.pipeline_state_name} t
            USING (
            SELECT
                '{self._cfg.pipeline_state_name}' AS pipeline_name,
                0L AS last_ingestion_ts_ms,
                cast(unix_millis(current_timestamp()) as bigint) AS updated_at_ms
            ) s
            ON t.pipeline_name = s.pipeline_name
            WHEN NOT MATCHED THEN INSERT *
        """)

    def ingest(self, startdate: str, enddate: str):
        sc = self._spark.sparkContext
        pages_acc = sc.accumulator(0)
        num_pages = self._noaa.fetch_number_of_pages(startdate, enddate)
        offsets = [1 + i * NOAAConfig().default_limit for i in range(num_pages)]
        print(f"xxxxxxxxxxxxxxxxxxxxxxxx offsets={offsets}")
        print(f"xxxxxxxxxxxxxxxxxxxxxxxx num_pages={num_pages}")

        def fetch_partition(offsets: Iterator[int]) -> Iterator[Row]:
            """
            One NOAAClient per Spark partition
            """
            client = NOAAClient(NOAAConfig())

            def log(msg: str):
                # stderr tends to show up more reliably in Spark logs
                print(
                    f"[FETCH host={host} pid={pid}] {msg}", file=sys.stderr, flush=True
                )

            fetched_pages = 0
            pages_acc.add(1)  # accumulate page fetch

            for offset in offsets:
                fetched_pages += 1
                log(f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx offset={offset}")
                rows = client.get_page(
                    os.getenv("PIPELINE_START_DAY", "2010-05-01"),
                    os.getenv("PIPELINE_END_DAY", "2010-05-31"),
                    client.cfg.default_limit,
                    offset,
                )
                for r in rows:
                    yield Row(
                        date=r["date"],
                        datatype=r.get("datatype"),
                        station=r.get("station"),
                        attributes=r.get("attributes"),
                        value=r.get("value"),
                    )

        rdd = self._spark.sparkContext.parallelize(offsets, 10)

        df_raw = self._spark.createDataFrame(
            rdd.mapPartitions(fetch_partition),
            schema=self.RAW_SCHEMA,
        )

        df_raw.limit(1).collect()
        print(f"pages fetched = {pages_acc.value}/{num_pages}", flush=True)

        good, bad = normalize_and_validate(df_raw)
        good_p = good.persist(StorageLevel.MEMORY_AND_DISK)
        bad_p = bad.persist(StorageLevel.MEMORY_AND_DISK)
        good_has = len(good_p.take(1)) > 0
        bad_has = len(bad_p.take(1)) > 0

        if good_has:
            print("start write good xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            (
                good.writeTo(
                    f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.good_table_name}"
                ).overwritePartitions()
            )
            print("done write good xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        if bad_has:
            print("start write bad xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            bad.writeTo(
                f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.bad_table_name}"
            ).append()
            print("done write bad xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        good_p.unpersist()
        bad_p.unpersist()

    def transform(self):
        def __get_last_watermark() -> int:
            row = (
                self._spark.table(
                    f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.pipeline_state_name}"
                )
                .filter(col("pipeline_name") == lit(self._cfg.pipeline_state_name))
                .select("last_ingestion_ts_ms")
                .limit(1)
                .collect()
            )
            return int(row[0]["last_ingestion_ts_ms"]) if row else 0

        def __set_last_watermark(new_wm: int) -> None:
            now_ms = int(
                self._spark.sql(
                    "SELECT cast(unix_millis(current_timestamp()) as bigint) AS x"
                ).collect()[0]["x"]
            )
            self._spark.sql(f"""
                MERGE INTO {self._cfg.catalog}.{self._cfg.db}.{self._cfg.pipeline_state_name} t
                USING (SELECT
                        '{self._cfg.pipeline_state_name}' AS pipeline_name,
                        {new_wm} AS last_ingestion_ts_ms,
                        {now_ms} AS updated_at_ms
                    ) s
                ON t.pipeline_name = s.pipeline_name
                WHEN MATCHED THEN UPDATE SET
                last_ingestion_ts_ms = s.last_ingestion_ts_ms,
                updated_at_ms = s.updated_at_ms
                WHEN NOT MATCHED THEN INSERT *
            """)

        last_wm = __get_last_watermark()

        src = (
            self._spark.table(
                f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.good_table_name}"
            )
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
        self._spark.sql(f"""
            MERGE INTO {self._cfg.catalog}.{self._cfg.db}.{self._cfg.missing_metrics_table_name} t
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
        __set_last_watermark(int(new_wm))

    def maintain(self):
        for table in [
            f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.missing_metrics_table_name}",
            f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.good_table_name}",
            f"{self._cfg.catalog}.{self._cfg.db}.{self._cfg.pipeline_state_name}",
        ]:
            self._spark.sql(f"""
                CALL iceberg.system.rewrite_data_files(
                table => '{table}',
                options => map('target-file-size-bytes','{self._cfg.tgt_size}')
                )
            """)

            self._spark.sql(f"""
                CALL iceberg.system.expire_snapshots(
                table => '{table}',
                retain_last => {self._cfg.snapshot_days}
                )
            """)
