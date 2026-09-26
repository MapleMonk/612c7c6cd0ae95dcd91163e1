{{ config(
            materialized='table',
                post_hook={
                    "sql": "create table if not exists `zouk-wh.MapleMonk.finance_ccogs_instamart` ( channel_name String, ccogs float64, ccogs_date date ); MERGE INTO `zouk-wh.MapleMonk.finance_ccogs_instamart` AS target USING ( SELECT \'INSTAMART_MANUAL_2\' AS channel_name, SUM(SAFE_CAST(discount_spend AS FLOAT64)) AS ccogs, DATE(order_date) AS ccogs_date FROM `zouk-wh.MapleMonk.Swiggy_CCOGS_brand_funded_discounts` GROUP BY DATE(order_date) ) AS source ON target.channel_name = source.channel_name AND target.ccogs_date = source.ccogs_date WHEN MATCHED THEN UPDATE SET target.ccogs = source.ccogs WHEN NOT MATCHED THEN INSERT ( channel_name, ccogs, ccogs_date ) VALUES ( source.channel_name, source.ccogs, source.ccogs_date );",
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
            