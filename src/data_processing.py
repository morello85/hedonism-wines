import duckdb
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#@st.cache_data
def create_or_replace_tables(folder_path, db_path):
    start_time = time.time()

    try:
        # Establish a connection to the DuckDB database
        with duckdb.connect(database=os.fspath(db_path)) as conn:
            # Set parallelism if your system supports it
            conn.execute('PRAGMA threads=4;')  # Adjust number of threads based on your system

            csv_glob = os.path.join(os.fspath(folder_path), "*.csv")
            start_time = time.time()
            conn.execute(
                """
                CREATE OR REPLACE TEMP VIEW raw AS
                SELECT *
                FROM read_csv_auto(
                    ?,
                    union_by_name=true,
                    filename=true
                )
                """,
                [csv_glob],
            )

            columns = {
                row[1] for row in conn.execute("PRAGMA table_info('raw')").fetchall()
            }

            def column_expr(possible_names, cast_type=None):
                expressions = []
                for name in possible_names:
                    if name in columns:
                        expr = f'raw."{name}"'
                        if cast_type:
                            expr = f"TRY_CAST({expr} AS {cast_type})"
                        expressions.append(expr)
                if not expressions:
                    return f"CAST(NULL AS {cast_type})" if cast_type else "NULL"
                if len(expressions) == 1:
                    return expressions[0]
                return f"COALESCE({', '.join(expressions)})"

            conn.execute(
                f"""
                CREATE OR REPLACE TABLE stocks_table AS
                SELECT
                    {column_expr(['ABV', 'abv'], 'DOUBLE')} AS abv,
                    {column_expr(['Available', 'available', 'availability'])} AS availability,
                    {column_expr(['Code', 'code'])} AS code,
                    {column_expr(['Country', 'country'])} AS country,
                    {column_expr(['Group', 'type'])} AS type,
                    {column_expr(['Link', 'url'])} AS url,
                    {column_expr(['Price (GBP)', 'price_gbp'], 'DOUBLE')} AS price_gbp,
                    {column_expr(['Price (ex-VAT)', 'price_ex_vat'], 'DOUBLE')} AS price_ex_vat,
                    {column_expr(['Price (inc VAT)', 'price_incl_vat'], 'DOUBLE')} AS price_incl_vat,
                    {column_expr(['Size', 'size'])} AS size,
                    {column_expr(['Style', 'style'])} AS style,
                    {column_expr(['Title', 'title'])} AS title,
                    {column_expr(['Vintage', 'vintage'])} AS vintage,
                    TRY_STRPTIME(
                        regexp_extract(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1),
                        '%Y_%m_%d'
                    )::DATE AS import_date
                FROM raw
                """
            )

            end_time = time.time()
            logger.info("Main table recreated taking %s seconds to run.", end_time - start_time)

            conn.execute("DROP VIEW IF EXISTS whisky_stocks_table")
            logger.info("whisky_stocks_table view dropped successfully.")
            # Create or replace the whisky_stocks_table view
            conn.execute("""CREATE OR REPLACE VIEW whisky_stocks_table AS 
                            SELECT 
                            abv,
                            availability,
                            code,
                            country,
                            type,
                            url,
                            COALESCE(price_gbp, price_incl_vat) AS price_gbp,
                            COALESCE(price_ex_vat,0) price_ex_vat,
                            COALESCE(price_incl_vat,0) price_incl_vat,
                            size,
                            style,
                            title,
                            vintage,
                            import_date
                            FROM stocks_table 
                            WHERE type = 'Whisky'""")
            
            conn.execute("DROP VIEW IF EXISTS whisky_stocks_table_today")
            logger.info("whisky_stocks_table_today view dropped successfully.")
            # Create or replace the whisky_stocks_table_today view
            conn.execute("""CREATE OR REPLACE VIEW whisky_stocks_table_today AS 
                            SELECT 
                            import_date, 
                            code,
                            title,
                            price_gbp,
                            url						  
                            FROM whisky_stocks_table 
                            WHERE import_date = CURRENT_DATE()
                            """)
            logger.info("Main views recreated.")
        
        logger.info("Tables created or replaced successfully.")
    except Exception as e:
        logger.exception("Error occurred: %s", e)
    end_time = time.time()
    logger.info("This function took %s seconds to run.", end_time - start_time)
