{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.META_AD_DAILY_V2_html COPY GRANTS AS select * from snitch_db.maplemonk.META_AD_DAILY_V2 ; CREATE OR REPLACE TABLE snitch_db.maplemonk.META_CONTENT_TABLE_html COPY GRANTS AS select * from snitch_db.maplemonk.META_CONTENT_TABLE ; CREATE OR REPLACE TABLE snitch_db.maplemonk.ALL_COMP_html COPY GRANTS AS select * from snitch_db.maplemonk.ALL_COMP ; CREATE OR REPLACE TABLE snitch_db.maplemonk.COMP_STATICS_DUMP_html COPY GRANTS AS select * from snitch_db.maplemonk.COMP_STATICS_DUMP ; CREATE OR REPLACE TABLE snitch_db.maplemonk.CONTENT_THROUGHPUT_TRACKER_MASTER_SHEET_html COPY GRANTS AS select * from snitch_db.maplemonk.CONTENT_THROUGHPUT_TRACKER_MASTER_SHEET ; CREATE OR REPLACE TABLE snitch_db.maplemonk.META_AUDIENCE_SPENDS_html COPY GRANTS AS select * from snitch_db.maplemonk.META_AUDIENCE_SPENDS ; CREATE OR REPLACE TABLE snitch_db.maplemonk.META_DOD_REGION_WISE_SPENDS_SALES_html COPY GRANTS AS select * from snitch_db.maplemonk.META_DOD_REGION_WISE_SPENDS_SALES ; CREATE OR REPLACE TABLE snitch_db.maplemonk.V_META_PRODUCT_SPENDS_CATALOGUE_html COPY GRANTS AS select * from snitch_db.maplemonk.V_META_PRODUCT_SPENDS_CATALOGUE ; CREATE TABLE IF NOT EXISTS SNITCH_DB.MAPLEMONK.DASH_CONTENT_META ( REFRESHED_AT TIMESTAMP_TZ NOT NULL, PERF_ROWS INTEGER NOT NULL ); INSERT INTO SNITCH_DB.MAPLEMONK.DASH_CONTENT_META (REFRESHED_AT, PERF_ROWS) SELECT CURRENT_TIMESTAMP(), COUNT(*) FROM SNITCH_DB.MAPLEMONK.META_AD_DAILY_V2_HTML; ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            