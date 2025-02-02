import os
import json
import time
import requests
from loguru import logger
from zoneinfo import ZoneInfo
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


DATA_DIR = Path("~/Projects/sov-data/data").expanduser()


@dataclass
class PolygonConfig:
    api_key: str
    base_url: str = "https://api.polygon.io/v2"


def get_bars(
    config: PolygonConfig,
    ticker: str,
    start_date: datetime,
    end_date: datetime,
    timespan: str = "hour",
    multiplier: int = 1,
    adjusted: bool = True,
    sort: str = "asc",
) -> list:
    try:
        endpoint = f"{config.base_url}/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"

        params: Dict[str, str | int | bool] = {
            "adjusted": str(adjusted).lower(),
            "sort": sort,
            "apiKey": config.api_key,
        }

        response = requests.get(endpoint, params=params)
        response.raise_for_status()

        data = response.json()
        bars = data["results"]
        if "next_url" in data:
            logger.info(f"Fetching more bars for {ticker}")
            next_url = data["next_url"]
            while next_url:
                response = requests.get(next_url, params=params)
                response.raise_for_status()
                data = response.json()
                bars.extend(data["results"])
                if "next_url" in data:
                    next_url = data["next_url"]
                else:
                    next_url = None
        return bars
    except Exception as e:
        logger.info(f"Failed to fetch bars for {ticker}: {e}")
        return []


def main():
    timespan = "hour"
    db_table = "daily_bars" if timespan == "day" else "bars"

    df = pd.read_csv(DATA_DIR / "s&p500" / "s&p500-constituents.csv")
    TICKERS = df["Symbol"].tolist()

    engine = create_engine("postgresql://:@localhost:5432/sp500")
    config = PolygonConfig(api_key="yO8J_rooAZkTTpRjl6H4PeKsoo1Qf2Z3")
    start_date = datetime(2023, 1, 1, tzinfo=ZoneInfo("America/New_York"))
    end_date = datetime(2024, 1, 1, tzinfo=ZoneInfo("America/New_York"))

    # for i, ticker in enumerate(["AAPL"]):
    for i, ticker in enumerate(TICKERS):
        start = time.time()
        logger.info(
            f"Downloading {db_table} for {ticker} ({i}) from {start_date.date()} to {end_date.date()}"
        )
        bars = get_bars(config, ticker, start_date, end_date, timespan=timespan)
        df = pd.DataFrame(bars)
        df.rename(
            columns={
                "t": "datetime_utc" if timespan == "hour" else "day",
                "o": "open",
                "c": "close",
                "h": "high",
                "l": "low",
                "v": "volume",
                "n": "trade_count",
                "vw": "vwap",
            },
            inplace=True,
        )
        if not df.empty:
            df["symbol"] = ticker
            if timespan == "hour":
                df["datetime_utc"] = pd.to_datetime(
                    df["datetime_utc"], unit="ms", utc=True
                )
                df["month"] = df["datetime_utc"].dt.month
                df["week"] = df["datetime_utc"].dt.isocalendar().week
            else:
                df["day"] = pd.to_datetime(df["day"], unit="ms", utc=True)

            logger.info(f"Downloaded {len(bars)} bars for {ticker}")
            print(df.head())
            print(df.tail())

            df.to_sql(
                db_table,
                engine,
                if_exists="append",
                index=False,
            )
        end = time.time()
        # Log time taken to download bars for a ticker
        logger.info(
            f"Time taken to download bars for {ticker}: {end - start:.2f} seconds"
        )


if __name__ == "__main__":
    main()
