{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ugaoo_db.maplemonk.Product_Funnel_Visualization_Ugaoo as select GA_DATE as Date,VIEW_ID, GA_SOURCE, GA_PRODUCTSKU, GA_PRODUCTNAME, Metric_Name as Stage , Metric_Value from (select GA_DATE, VIEW_ID, GA_USERS as Users, GA_SOURCE, GA_PRODUCTSKU, GA_PRODUCTNAME, GA_UNIQUEPURCHASES as Purchases, GA_PRODUCTCHECKOUTS as Checkouts, GA_PRODUCTADDSTOCART::int AddsToCart, GA_PRODUCTDETAILVIEWS from ugaoo_db.maplemonk.GOOGLEANALYTICS_UGAOO_IN_PRODUCT_FUNNEL__UGAOO) unpivot (Metric_Value for Metric_Name in (Users , AddsToCart ,Checkouts ,Purchases )) order by ga_date desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from UGAOO_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        