{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.skinbae_tira_fact_items as SELECT ItemCode, ProdName, brandname, PARSE_DATE(\'%b %e, %Y\', TRIM(Order_Date, \'\"\')) AS Order_Date, CAST(booked_qty AS INT64) AS booked_qty, l1_category, l2_category, l3_category, CAST(booked_revenue AS FLOAT64) AS booked_revenue, CAST(booked_mrp_revenue AS FLOAT64) AS booked_mrp_revenue, FROM maplemonk.skinbae_s3_tira_sales ;",
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
            