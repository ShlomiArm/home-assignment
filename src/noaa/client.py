from urllib3.util.retry import Retry
from requests import Session, adapters, HTTPError
import math
from config import NOAAConfig


class NOAAClient:
    def __init__(self, cfg: NOAAConfig):
        self.cfg = cfg
        # --- Retry-enabled session ---
        retry = Retry(
            total=8,  # total retries across all errors
            connect=3,  # retries on connection errors
            read=3,  # retries on read errors
            status=5,  # retries on HTTP status codes below
            backoff_factor=0.5,  # 0.5s, 1s, 2s, 4s, ... (with jitter internally)
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,  # we will raise manually after the request
            respect_retry_after_header=True,  # honor Retry-After for 429 if present
        )

        adapter = adapters.HTTPAdapter(
            max_retries=retry, pool_connections=10, pool_maxsize=10
        )
        self.session = Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def __page(self, start_date: str, end_date: str, limit: int, offset: int = 1):
        headers = {"token": self.cfg.token}

        params: dict[str, str] = {
            "datasetid": self.cfg.dataset_id,
            "startdate": start_date,
            "enddate": end_date,
            "offset": str(offset),
            "limit": str(limit),
        }

        resp = self.session.get(
            self.cfg.base_url,
            headers=headers,
            params=params,
            timeout=self.cfg.timeout_s,
        )

        # If still failing after retries, raise a helpful error
        if resp.status_code >= 400:
            # show some payload text to debug (NOAA often returns JSON errors)
            snippet = (resp.text or "")[:500]
            raise HTTPError(
                f"NOAA request failed (status={resp.status_code}) offset={offset} "
                f"params={params}. Response: {snippet}",
                response=resp,
            )

        payload = resp.json()
        return payload.get("results") or [], payload.get("metadata", {}).get(
            "resultset", {}
        ).get("count", 0)

    def fetch_number_of_pages(self, start_date: str, end_date: str):
        _, meta = self.__page(start_date, end_date, self.cfg.default_limit, 1)
        return math.ceil(meta / self.cfg.default_limit)

    def get_page(self, start_date: str, end_date: str, limit: int, offset: int = 1):
        rows, _ = self.__page(start_date, end_date, limit, offset)
        return rows
