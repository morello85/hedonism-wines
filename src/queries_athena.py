from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "hedonism_wines")
ATHENA_REGION = os.getenv("AWS_REGION", "eu-west-1")
ATHENA_OUTPUT_LOCATION = os.getenv(
    "ATHENA_QUERY_RESULTS_S3",
    "s3://dario-athena-query-results/",
)

athena_client = boto3.client("athena", region_name=ATHENA_REGION)


def _run_query(query: str, poll_interval: int = 2, timeout_seconds: int = 300) -> pd.DataFrame:
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    query_execution_id = response["QueryExecutionId"]

    start_time = time.time()
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}

    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = execution["QueryExecution"]["Status"]["State"]

        if status in terminal_states:
            if status != "SUCCEEDED":
                reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query {query_execution_id} {status}: {reason}")
            break

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Athena query {query_execution_id} timed out")

        time.sleep(poll_interval)

    paginator = athena_client.get_paginator("get_query_results")
    rows: list[list[str | None]] = []
    column_info = None
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        result_set = page["ResultSet"]
        if column_info is None:
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]

        for row in result_set["Rows"]:
            data = [cell.get("VarCharValue") for cell in row["Data"]]
            rows.append(data)

    if not rows or column_info is None:
        return pd.DataFrame()

    headers = [col["Name"] for col in column_info]
    body = rows[1:] if rows and rows[0] == headers else rows
    return pd.DataFrame(body, columns=headers)


def query_discounted_items() -> pd.DataFrame:
    query = """
            SELECT
                c.import_date AS current_import_date,
                c.code,
                c.title,
                c.url,
                CAST(c.price_gbp AS DOUBLE) AS current_price,
                m.max_price AS old_price,
                m.max_price - CAST(c.price_gbp AS DOUBLE) AS discount,
                ROUND(
                    ((m.max_price - CAST(c.price_gbp AS DOUBLE)) / NULLIF(m.max_price, 0)) * 100,
                    4
                ) AS perc_saving
            FROM whisky_stocks_view_today c
            JOIN (
                SELECT code, MAX(CAST(price_gbp AS DOUBLE)) AS max_price
                FROM whisky_stocks_view
                GROUP BY code
            ) m ON c.code = m.code
            WHERE m.max_price - CAST(c.price_gbp AS DOUBLE) > 0
            ORDER BY discount DESC;

    """
    df = _run_query(query)
    numeric_cols = ["current_price", "old_price", "discount", "perc_saving"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def stocks_and_median_values() -> pd.DataFrame:
    query = """
        SELECT COUNT(*) AS stock_count,
               MEDIAN(CAST(price_gbp AS DOUBLE)) AS median_price,
               SUM(CAST(availability AS DOUBLE)) AS total_availability,
               import_date
        FROM whisky_stocks_view
        GROUP BY import_date
        ORDER BY 3 DESC
    """
    df = _run_query(query)
    for col in ["stock_count", "median_price", "total_availability"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "import_date" in df.columns:
        df["import_date"] = pd.to_datetime(df["import_date"]).dt.date.astype(str).str[:10]
    return df


def stocks_and_median_values_by_code() -> pd.DataFrame:
    query = """
        WITH x AS (
            SELECT
                MEDIAN(CAST(price_gbp AS DOUBLE)) AS median_price,
                SUM(CAST(availability AS DOUBLE)) AS total_availability,
                import_date,
                code
            FROM whisky_stocks_view
            GROUP BY import_date, code
            ORDER BY 3 DESC
        ),
        y AS (
            SELECT COUNT(DISTINCT median_price) AS price_changes_count,
                   code
            FROM x
            GROUP BY code
        )
        SELECT x.*, y.price_changes_count
        FROM x INNER JOIN y ON x.code = y.code
    """
    df = _run_query(query)
    for col in ["median_price", "total_availability", "price_changes_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "import_date" in df.columns:
        df["import_date"] = pd.to_datetime(df["import_date"]).dt.date.astype(str).str[:10]
    return df


def units_sold(output_folder: Optional[Path] = None) -> pd.DataFrame:
    query = """
        WITH todays_items AS (
            SELECT code, title, url, price_gbp, availability, import_date
            FROM whisky_stocks_view
            WHERE CAST(import_date AS DATE) = CURRENT_DATE
        ),
        yesterdays_items AS (
            SELECT code, title, url, price_gbp, availability, import_date
            FROM whisky_stocks_view
            WHERE CAST(import_date AS DATE) = DATE_ADD('day', -1, CURRENT_DATE)
        )
        SELECT CAST(CURRENT_DATE AS DATE) AS import_date,
               a.code,
               a.title,
               a.url,
               a.price_gbp,
               a.today_availability AS availability,
               CAST(a.yesterday_availability AS DOUBLE) - CAST(a.today_availability AS DOUBLE) AS units_sold
        FROM (
            SELECT y.code,
                   y.title,
                   y.url,
                   y.price_gbp,
                   y.availability AS yesterday_availability,
                   COALESCE(t.availability, 0) AS today_availability
            FROM yesterdays_items y
            LEFT JOIN todays_items t ON y.code = t.code
        ) a
        WHERE CAST(a.yesterday_availability AS DOUBLE) - CAST(a.today_availability AS DOUBLE) > 0
        ORDER BY price_gbp DESC
    """
    df = _run_query(query)
    for col in ["price_gbp", "availability", "units_sold"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "import_date" in df.columns:
        df["import_date"] = pd.to_datetime(df["import_date"]).dt.date.astype(str).str[:10]

    if output_folder:
        output_folder.mkdir(parents=True, exist_ok=True)
        filename = f"sales{datetime.now().strftime('_%Y_%m_%d')}.csv"
        df.to_csv(output_folder / filename, index=False)

    return df


def price_search() -> pd.DataFrame:
    query = """
        SELECT import_date,
               code,
               title,
               price_gbp,
               url
        FROM whisky_stocks_view_today
    """
    df = _run_query(query)
    if "price_gbp" in df.columns:
        df["price_gbp"] = pd.to_numeric(df["price_gbp"], errors="coerce")
    return df
