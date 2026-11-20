{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS `zouk-wh.MapleMonk.finance_ccogs_blinkit` ( channel_name STRING, ccogs FLOAT64, ccogs_date DATE ); DELETE FROM `zouk-wh.MapleMonk.finance_ccogs_blinkit` WHERE channel_name = \'BLINKIT_MANUAL\' AND ccogs_date IN ( SELECT DISTINCT DATE(date) FROM `MapleMonk.Blinkit_CCOGS_spends_summary` ); MERGE INTO `zouk-wh.MapleMonk.finance_ccogs_blinkit` AS target USING ( WITH Deduped AS ( SELECT DISTINCT date, city, product_id, item_id, multiplier, offer_type, item_mrp, brandfund_absolute_value, brandfund_absolute_input_value, brandfund_percentage_value, product_name, l0_category_name, l1_category_name, l2_category_name, brand_name, p_type, qty_sold, total_brand_fund, system_sheet_id, mrp_gmv, upload_source, user_email FROM `MapleMonk.Blinkit_CCOGS_spends_summary` ) SELECT \'BLINKIT_MANUAL\' AS channel_name, COALESCE(SUM(SAFE_CAST(total_brand_fund AS FLOAT64)), 0) AS ccogs, DATE(date) AS ccogs_date FROM Deduped GROUP BY DATE(date) ) AS source ON target.channel_name = source.channel_name AND target.ccogs_date = source.ccogs_date WHEN MATCHED THEN UPDATE SET target.ccogs = source.ccogs WHEN NOT MATCHED THEN INSERT (channel_name, ccogs, ccogs_date) VALUES (source.channel_name, source.ccogs, source.ccogs_date); SELECT DATE_TRUNC(ccogs_date, MONTH) AS month, COUNT(*) AS days, ROUND(SUM(ccogs), 2) AS ccogs FROM `zouk-wh.MapleMonk.finance_ccogs_blinkit` WHERE channel_name = \'BLINKIT_MANUAL\' GROUP BY month ORDER BY month;",
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
            