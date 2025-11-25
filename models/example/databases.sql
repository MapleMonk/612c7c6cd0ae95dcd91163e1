{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ga4_plus_size_data as WITH base AS ( SELECT PROPERTY_ID, TO_DATE(CAST(DATE AS STRING), \'YYYYMMDD\') AS DATE_, EVENTNAME, EVENTCOUNT FROM snitch_db.maplemonk.plus_size_event_count ) SELECT PROPERTY_ID, DATE_, SUM(CASE WHEN EVENTNAME = \'page_view\' THEN EVENTCOUNT END) AS page_view, SUM(CASE WHEN EVENTNAME = \'first_visit\' THEN EVENTCOUNT END) AS first_visit, SUM(CASE WHEN EVENTNAME = \'scroll\' THEN EVENTCOUNT END) AS scroll, SUM(CASE WHEN EVENTNAME = \'add_to_cart_plus_store\' THEN EVENTCOUNT END) AS add_to_cart_plus_store, SUM(CASE WHEN EVENTNAME = \'address_form_open\' THEN EVENTCOUNT END) AS address_form_open, SUM(CASE WHEN EVENTNAME = \'form_submit\' THEN EVENTCOUNT END) AS form_submit, SUM(CASE WHEN EVENTNAME = \'payment_expand_cod\' THEN EVENTCOUNT END) AS payment_expand_cod, SUM(CASE WHEN EVENTNAME = \'pdp_tap_fis\' THEN EVENTCOUNT END) AS pdp_tap_fis, SUM(CASE WHEN EVENTNAME = \'form_start\' THEN EVENTCOUNT END) AS form_start FROM base GROUP BY 1,2 ORDER BY 2,1;",
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
            