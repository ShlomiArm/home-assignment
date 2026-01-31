import os
from typing import Any, Dict, List, Tuple
from config import TrinoConfig
from trino.dbapi import connect


class TrinoIcebergWriter:
    def __init__(
        self,
        cfg: TrinoConfig
        
    ):
        self.cfg=cfg
        self.conn = connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            http_scheme=self.cfg.http_scheme,
            catalog=self.cfg.catalog,
        )
        self.cur = self.conn.cursor()

    # -------------------------
    # Helpers
    # -------------------------
    def _exec(self, sql: str):
        self.cur.execute(sql)
        try:
            self.cur.fetchall()
        except Exception:
            pass

    # -------------------------
    # Table setup
    # -------------------------
    def create_tables(self):
        self._exec(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{self.cfg.schema_name}")

        self._exec(f"""
        CREATE TABLE IF NOT EXISTS {self.cfg.schema_name}.noaa_precip_15 (
          date_epoch_ms bigint,
          datatype      varchar,
          station       varchar,
          attributes    varchar,
          value         integer,
          ingestion_ts  timestamp(3)
        )
        WITH (format='PARQUET', location = 's3://iceberg/{self.cfg.schema_name}/noaa_precip_15')
        """)

    # -------------------------
    # Insert batch
    # -------------------------
    @staticmethod
    def _rows_to_values(batch: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
        out = []
        for r in batch:
            out.append(
                (
                    r.get("date"),
                    r.get("datatype"),
                    r.get("station"),
                    r.get("attributes"),
                    None if r.get("value") is None else str(r.get("value")),
                )
            )
        return out

    def insert_batch(self, batch: List[Dict[str, Any]]):
        sql = f"""
        INSERT INTO {self.catalog}.bronze.noaa_raw
        (date, datatype, station, attributes, value)
        VALUES (?, ?, ?, ?, ?)
        """
        self.cur.executemany(sql, self._rows_to_values(batch))
        try:
            self.cur.fetchall()
        except Exception:
            pass

    # -------------------------
    # Validation + transform
    # -------------------------
    def transform_to_silver(self):
        self._exec(f"""
        INSERT INTO {self.catalog}.silver.noaa_precip_15
        SELECT
          CAST(to_unixtime(from_iso8601_timestamp(date))*1000 AS bigint),
          datatype,
          station,
          attributes,
          TRY_CAST(value AS integer),
          current_timestamp
        FROM {self.catalog}.bronze.noaa_raw
        WHERE
          date IS NOT NULL
          AND datatype IS NOT NULL
          AND station IS NOT NULL
          AND from_iso8601_timestamp(date) IS NOT NULL
          AND TRY_CAST(value AS integer) IS NOT NULL
        """)

    # -------------------------
    # Optional cleanup
    # -------------------------
    def truncate_staging(self):
        self._exec(f"DELETE FROM {self.catalog}.bronze.noaa_raw")

    # -------------------------
    # Close
    # -------------------------
    def close(self):
        self.cur.close()
        self.conn.close()
