{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Product_Funnel_Visualization_Snitch as select GA_DATE,VIEW_ID, GA_SOURCE, GA_PRODUCTSKU, GA_PRODUCTNAME, Metric_Name, Metric_Value from (select GA_DATE, VIEW_ID, GA_USERS as Users, GA_SOURCE, GA_PRODUCTSKU, GA_PRODUCTNAME, GA_UNIQUEPURCHASES as Purchases, GA_PRODUCTCHECKOUTS as Checkouts, GA_PRODUCTADDSTOCART::int AddsToCart, GA_PRODUCTDETAILVIEWS from snitch_db.maplemonk.google_analytics_product_funnel_snitch) unpivot (Metric_Value for Metric_Name in (Users , AddsToCart ,Checkouts ,Purchases )) order by ga_date desc;",
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
                        