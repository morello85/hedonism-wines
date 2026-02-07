from __future__ import annotations

import os
import time
from datetime import date, datetime
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
s3_client = boto3.client("s3", region_name=ATHENA_REGION)


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


def _execute_statement(statement: str, poll_interval: int = 2, timeout_seconds: int = 300) -> None:
    response = athena_client.start_query_execution(
        QueryString=statement,
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
                raise RuntimeError(f"Athena statement {query_execution_id} {status}: {reason}")
            return

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Athena statement {query_execution_id} timed out")

        time.sleep(poll_interval)


def _clear_s3_prefix(s3_uri: str) -> None:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {s3_uri}")

    bucket, _, prefix = s3_uri[5:].partition("/")
    list_prefix = prefix.rstrip("/")
    list_prefix = f"{list_prefix}/" if list_prefix else ""

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def athena_tables_creation() -> None:
    parquet_location = os.getenv("ATHENA_PARQUET_S3", "s3://hedonism-wines-api-parquet/")
    raw_csv_location = os.getenv("ATHENA_RAW_CSV_S3", "s3://hedonism-wines-api-files/")

    statements = [
        "DROP VIEW IF EXISTS whisky_stocks_view_today",
        "DROP VIEW IF EXISTS whisky_stocks_view",
        "DROP VIEW IF EXISTS stocks_view",
        "DROP TABLE IF EXISTS stocks_table",
        "DROP TABLE IF EXISTS stocks_table_raw",
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS stocks_table_raw (
            code STRING,
            title STRING,
            vintage STRING,
            size STRING,
            abv STRING,
            style STRING,
            country STRING,
            group_name STRING,
            available STRING,
            price_incl_vat STRING,
            price_ex_vat STRING,
            link STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar' = ',',
            'quoteChar' = '"',
            'escapeChar' = '\\\\'
        )
        STORED AS TEXTFILE
        LOCATION '{raw_csv_location}'
        TBLPROPERTIES ('skip.header.line.count' = '1')
        """,
        "DROP TABLE IF EXISTS stocks_table_parquet",
    ]

    create_parquet_table = f"""
        CREATE TABLE IF NOT EXISTS stocks_table_parquet
        WITH (
            format = 'PARQUET',
            external_location = '{parquet_location}'
        ) AS
        WITH raw AS (
            SELECT
                code,
                title,
                vintage,
                size,
                abv,
                style,
                country,
                group_name AS group_value,
                available,
                price_incl_vat,
                price_ex_vat,
                link,
                "$path" AS source_path,
                CASE
                    WHEN available IS NULL
                        AND price_incl_vat IS NULL
                        AND price_ex_vat IS NULL
                        AND link IS NULL
                        AND TRY_CAST(group_name AS DOUBLE) IS NOT NULL
                    THEN true
                    ELSE false
                END AS is_legacy_schema
            FROM {ATHENA_DATABASE}.stocks_table_raw
        )
        SELECT
            TRY_CAST(CASE WHEN is_legacy_schema THEN NULL ELSE abv END AS DOUBLE) AS abv,
            CASE WHEN is_legacy_schema THEN country ELSE available END AS availability,
            code,
            CASE WHEN is_legacy_schema THEN abv ELSE country END AS country,
            CASE WHEN is_legacy_schema THEN style ELSE group_value END AS type,
            CASE WHEN is_legacy_schema THEN NULL ELSE link END AS url,
            TRY_CAST(CASE WHEN is_legacy_schema THEN group_value ELSE NULL END AS DOUBLE) AS price_gbp,
            TRY_CAST(CASE WHEN is_legacy_schema THEN NULL ELSE price_ex_vat END AS DOUBLE) AS price_ex_vat,
            TRY_CAST(CASE WHEN is_legacy_schema THEN NULL ELSE price_incl_vat END AS DOUBLE) AS price_incl_vat,
            CASE WHEN is_legacy_schema THEN vintage ELSE size END AS size,
            CASE WHEN is_legacy_schema THEN size ELSE style END AS style,
            title,
            CASE WHEN is_legacy_schema THEN NULL ELSE vintage END AS vintage,
            DATE_PARSE(regexp_extract(source_path, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1), '%Y_%m_%d') AS import_date
        FROM raw
    """

    trailing_statements = [
        """
        CREATE VIEW stocks_view AS
        SELECT
            abv,
            availability,
            code,
            country,
            type,
            url,
            price_gbp,
            price_ex_vat,
            price_incl_vat,
            size,
            style,
            title,
            vintage,
            import_date
        FROM stocks_table_parquet
        """,
        """
        CREATE VIEW whisky_stocks_view AS
        SELECT
            abv,
            availability,
            code,
            country,
            type,
            url,
            COALESCE(price_gbp, price_incl_vat) AS price_gbp,
            COALESCE(price_ex_vat, 0) AS price_ex_vat,
            COALESCE(price_incl_vat, 0) AS price_incl_vat,
            size,
            style,
            title,
            vintage,
            import_date
        FROM stocks_table_parquet
        WHERE type = 'Whisky'
        """,
        """
        CREATE VIEW whisky_stocks_view_today AS
        SELECT
            import_date,
            code,
            title,
            price_gbp,
            url
        FROM whisky_stocks_view
        WHERE CAST(import_date AS DATE) = CURRENT_DATE
        """,
    ]

    for statement in statements:
        _execute_statement(statement)

    _clear_s3_prefix(parquet_location)
    _execute_statement(create_parquet_table)

    for statement in trailing_statements:
        _execute_statement(statement)

    print("Athena tables and views created successfully.")


def query_discounted_items() -> pd.DataFrame:
    query = """
            WITH price_changes AS (
                SELECT
                    code,
                    COUNT(DISTINCT median_price) AS price_changes_count
                FROM (
                    SELECT
                        code,
                        import_date,
                        approx_percentile(CAST(price_gbp AS DOUBLE), 0.5) AS median_price
                    FROM whisky_stocks_view
                    GROUP BY code, import_date
                ) grouped_prices
                GROUP BY code
            )
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
                ) AS perc_saving,
                p.price_changes_count
            FROM whisky_stocks_view_today c
            JOIN (
                SELECT code, MAX(CAST(price_gbp AS DOUBLE)) AS max_price
                FROM whisky_stocks_view
                GROUP BY code
            ) m ON c.code = m.code
            LEFT JOIN price_changes p ON c.code = p.code
            WHERE m.max_price - CAST(c.price_gbp AS DOUBLE) > 0
            ORDER BY discount DESC;

    """
    df = _run_query(query)
    numeric_cols = ["current_price", "old_price", "discount", "perc_saving", "price_changes_count"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def stocks_and_median_values() -> pd.DataFrame:
    query = """
        SELECT COUNT(*) AS stock_count,
               approx_percentile(CAST(price_gbp AS DOUBLE), 0.5) AS median_price,
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


def _escape_sql_string(value: str) -> str:
    """Escape a string for inclusion in Athena SQL string literals."""
    return value.replace("'", "''")


def stocks_and_median_values_by_code(codes: list[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["median_price", "total_availability", "import_date", "code", "price_changes_count"])

    escaped_codes = [f"'{_escape_sql_string(code)}'" for code in codes]
    codes_clause = ", ".join(escaped_codes)
    query = f"""
        WITH filtered AS (
            SELECT
                approx_percentile(CAST(price_gbp AS DOUBLE), 0.5) AS median_price,
                SUM(CAST(availability AS DOUBLE)) AS total_availability,
                import_date,
                code
            FROM whisky_stocks_view
            WHERE code IN ({codes_clause})
            GROUP BY import_date, code
        ),
        changes AS (
            SELECT COUNT(DISTINCT median_price) AS price_changes_count,
                   code
            FROM filtered
            GROUP BY code
        )
        SELECT f.*, c.price_changes_count
        FROM filtered f
        INNER JOIN changes c ON f.code = c.code
        ORDER BY f.import_date DESC
    """
    df = _run_query(query)
    for col in ["median_price", "total_availability", "price_changes_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "import_date" in df.columns:
        df["import_date"] = pd.to_datetime(df["import_date"]).dt.date.astype(str).str[:10]
    return df


def _normalise_target_date(target_date: Optional[date | datetime | str]) -> str:
    if target_date is None:
        return datetime.now().date().isoformat()
    if isinstance(target_date, datetime):
        return target_date.date().isoformat()
    if isinstance(target_date, date):
        return target_date.isoformat()

    parsed = datetime.fromisoformat(target_date)
    return parsed.date().isoformat()


def units_sold_by_date(
    target_date: date | datetime | str,
    output_folder: Optional[Path] = None,
) -> pd.DataFrame:
    target_date_value = _normalise_target_date(target_date)
    query = f"""
        WITH price_changes AS (
            SELECT
                code,
                COUNT(DISTINCT median_price) AS price_changes_count
            FROM (
                SELECT
                    code,
                    import_date,
                    approx_percentile(CAST(price_gbp AS DOUBLE), 0.5) AS median_price
                FROM whisky_stocks_view
                GROUP BY code, import_date
            ) grouped_prices
            GROUP BY code
        ),
        params AS (
            SELECT DATE '{target_date_value}' AS target_date
        ),
        current_items AS (
            SELECT code, title, url, price_gbp, availability
            FROM whisky_stocks_view
            CROSS JOIN params
            WHERE CAST(import_date AS DATE) = target_date
        ),
        previous_items AS (
            SELECT code, title, url, price_gbp, availability
            FROM whisky_stocks_view
            CROSS JOIN params
            WHERE CAST(import_date AS DATE) = DATE_ADD('day', -1, target_date)
        )
        SELECT prm.target_date AS import_date,
               a.code,
               a.title,
               a.url,
               a.price_gbp,
               a.current_availability AS availability,
               a.previous_availability - a.current_availability AS units_sold,
               pc.price_changes_count
        FROM (
            SELECT y.code,
                   y.title,
                   y.url,
                   y.price_gbp,
                   COALESCE(TRY_CAST(y.availability AS DOUBLE), 0.0) AS previous_availability,
                   COALESCE(TRY_CAST(t.availability AS DOUBLE), 0.0) AS current_availability
            FROM previous_items y
            LEFT JOIN current_items t ON y.code = t.code
        ) a
        LEFT JOIN price_changes pc ON a.code = pc.code
        CROSS JOIN params prm
        WHERE a.previous_availability - a.current_availability > 0
        ORDER BY price_gbp DESC
    """
    df = _run_query(query)
    for col in ["price_gbp", "availability", "units_sold", "price_changes_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "import_date" in df.columns:
        df["import_date"] = pd.to_datetime(df["import_date"]).dt.date.astype(str).str[:10]

    if output_folder:
        output_folder.mkdir(parents=True, exist_ok=True)
        filename = f"sales_{target_date_value}.csv"
        df.to_csv(output_folder / filename, index=False)

    return df


def previous_day_units_sold(output_folder: Optional[Path] = None) -> pd.DataFrame:
    return units_sold_by_date(datetime.now().date(), output_folder=output_folder)


def price_search() -> pd.DataFrame:
    query = """
        WITH price_changes AS (
            SELECT
                code,
                COUNT(DISTINCT median_price) AS price_changes_count
            FROM (
                SELECT
                    code,
                    import_date,
                    approx_percentile(CAST(price_gbp AS DOUBLE), 0.5) AS median_price
                FROM whisky_stocks_view
                GROUP BY code, import_date
            ) grouped_prices
            GROUP BY code
        )
        SELECT c.import_date,
               c.code,
               c.title,
               c.price_gbp,
               c.url,
               p.price_changes_count
        FROM whisky_stocks_view_today c
        LEFT JOIN price_changes p ON c.code = p.code
    """
    df = _run_query(query)
    for col in ["price_gbp", "price_changes_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
