from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class NOAAConfig:
    base_url: str = "http://www.ncei.noaa.gov/cdo-web/api/v2/data"
    timeout_s: int = 60
    default_limit: int = 1000
    sleep_s: float = 0.1
    dataset_id: str = "PRECIP_15"
    token: Optional[str] = os.getenv("NOAA_TOKEN")


@dataclass
class PipelineConfig:
    catalog: str = "iceberg"
    db: str = "assigment"
    good_table_name: str = "good"
    bad_table_name: str = "bad"
    missing_metrics_table_name: str = "missing_metrics"
    pipeline_state_name: str = "pipeline_state"
    chunkdays: int = 10
    tgt_size: int = 128 * 1024 * 1024
    snapshot_days: int = 3
