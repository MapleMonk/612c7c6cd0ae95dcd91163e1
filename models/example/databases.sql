{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Monthly_Inventory_Inwards_Sales_Report` AS WITH snap AS ( SELECT SAFE_CAST(SUBSTR(CAST(DATA_FETCH_DATE AS STRING), 1, 10) AS DATE) AS fetch_day, DATA_FETCH_DATE AS fetch_ts, SAFE_CAST(REPLACE(TRIM(CAST(Available_Inventory AS STRING)), \',\', \'\') AS FLOAT64) AS available_inventory FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` ), snap_m AS ( SELECT DATE_TRUNC(fetch_day, MONTH) AS month, fetch_day, fetch_ts, available_inventory FROM snap WHERE fetch_day IS NOT NULL ), first_fetch AS ( SELECT month, fetch_ts FROM (SELECT DISTINCT month, fetch_day, fetch_ts FROM snap_m) WHERE TRUE QUALIFY ROW_NUMBER() OVER ( PARTITION BY month ORDER BY fetch_day, fetch_ts DESC ) = 1 ), start_inv AS ( SELECT s.month, SUM(IFNULL(s.available_inventory, 0)) AS start_inventory FROM snap_m s JOIN first_fetch f ON s.month = f.month AND s.fetch_ts = f.fetch_ts GROUP BY s.month ), inwards AS ( SELECT DATE_TRUNC( COALESCE( SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(CAST(Date AS STRING))), SAFE_CAST(SUBSTR(CAST(Date AS STRING), 1, 10) AS DATE) ), MONTH) AS month, SUM(IFNULL(SAFE_CAST(REPLACE(TRIM(CAST(Inward_Quantity AS STRING)), \',\', \'\') AS FLOAT64), 0)) AS total_inwards FROM `MapleMonk.Inward_Data` GROUP BY month ), sales AS ( SELECT DATE_TRUNC(SAFE_CAST(SUBSTR(CAST(Date AS STRING), 1, 10) AS DATE), MONTH) AS month, SUM(IFNULL(SAFE_CAST(CAST(quantity AS STRING) AS FLOAT64), 0)) AS gross_qty_sold, SUM(IFNULL(SAFE_CAST(CAST(return_quantity AS STRING) AS FLOAT64), 0)) AS qty_returned FROM `Maplemonk.zouk_pandl` WHERE SKU IS NOT NULL GROUP BY month ) SELECT COALESCE(i.month, w.month, s.month) AS month, FORMAT_DATE(\'%b %Y\', COALESCE(i.month, w.month, s.month)) AS month_label, IFNULL(i.start_inventory, 0) AS start_inventory, IFNULL(w.total_inwards, 0) AS total_inwards, IFNULL(s.gross_qty_sold, 0) AS gross_qty_sold, IFNULL(s.qty_returned, 0) AS qty_returned, IFNULL(s.gross_qty_sold, 0) - IFNULL(s.qty_returned, 0) AS net_qty_sold FROM start_inv i FULL OUTER JOIN inwards w USING (month) FULL OUTER JOIN sales s USING (month) WHERE COALESCE(i.month, w.month, s.month) IS NOT NULL;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            