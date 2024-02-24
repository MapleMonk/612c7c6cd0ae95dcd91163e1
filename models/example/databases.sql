{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SHOPIFY_SKULEVEL_PAID AS WITH s1 AS ( SELECT *, REVERSE(SUBSTRING(REVERSE(PAGE_PATH), 1, CHARINDEX(\'/\', REVERSE(PAGE_PATH)) - 1)) AS HANDLE FROM SELECT_DB.MAPLEMONK.SELECT_DB_SHOPIFY_PAID_SESSIONS ) SELECT s1.DAY, s1.MONTH, s1.HANDLE, s1.UA_BROWSER, s1.TOTAL_CARTS, s1.LOCATION_CITY, s1.TOTAL_SESSIONS, s1.TOTAL_VISITORS, s1.UA_FORM_FACTOR, s1.TOTAL_CHECKOUTS, s1.TOTAL_CONVERSION, s1.REFERRING_CHANNEL, s1.REFERRING_TRAFFIC, s1.TOTAL_BOUNCE_RATE, s1.REFERRING_CATEGORY, s1.TOTAL_ORDERS_PLACED, s1.UTM_CAMPAIGN_MEDIUM, s1.UTM_CAMPAIGN_SOURCE, s1._AB_SOURCE_FILE_LAST_MODIFIED, a1.\"Parent Category\", a1.\"Child Category\", a1.\"Parent SKU\" FROM s1 JOIN SELECT_DB.MAPLEMONK.SELECT_DB_SHOPIFY_DATA_MASTER AS a1 ON s1.HANDLE = a1.HANDLE;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        