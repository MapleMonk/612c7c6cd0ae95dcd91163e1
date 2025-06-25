{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.inwards_date_and_quantity_eoq AS WITH dod_inv AS ( SELECT date, sku_group, SUM(inv) AS inv FROM snitch_db.maplemonk.dod_all_channels_sku_inventory GROUP BY 1, 2 ), inv_with_change AS ( SELECT *, inv - LAG(inv) OVER (PARTITION BY sku_group ORDER BY date) AS inv_change FROM dod_inv ), inward_starts AS ( SELECT sku_group, date AS start_date FROM inv_with_change WHERE inv_change >= 300 ), inward_windows AS ( SELECT s.sku_group, s.start_date, d.date AS peak_date, d.inv, ROW_NUMBER() OVER ( PARTITION BY s.sku_group, s.start_date ORDER BY d.inv DESC, d.date DESC ) AS rn FROM inward_starts s JOIN dod_inv d ON s.sku_group = d.sku_group AND d.date BETWEEN s.start_date AND DATEADD(day, 30, s.start_date) ), final_inwards_ranked AS ( SELECT * FROM inward_windows WHERE rn = 1 ), deduplicated_inwards AS ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY sku_group ORDER BY peak_date ) AS inward_event_id FROM final_inwards_ranked ) SELECT sku_group, start_date AS inward_start_date, peak_date AS inward_date, inv AS inventory_on_inward_date FROM deduplicated_inwards qualify row_number() over (partition by sku_group,inward_date order by inv desc) = 1 ORDER BY sku_group, inward_date ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            