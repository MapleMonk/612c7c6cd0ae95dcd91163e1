{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.Amazon_Last_Year_Amazonads_Vahdam as select a.date::date as Date ,ly.date::date as Last_year_date ,a.Region ,a.ASIN_NEW ,a.COMMONSKU_ID ,a.BRAND ,a.PRODUCTNAME ,sum(a.ORDERS) as Orders ,sum(a.QUANTITY) as Quantity ,sum(a.SALES_USD) as Sales ,sum(a.SPEND) as Spend ,sum(a.SESSIONS) as Sessions ,sum(ly.ORDERS) as LY_Orders ,sum(ly.QUANTITY) as LY_Quantity ,sum(ly.SALES_USD) as LY_Sales ,sum(ly.SPEND) as LY_Spend ,sum(ly.SESSIONS) as LY_Sessions From vahdam_db.maplemonk.amazonads_overall_marketing a left join vahdam_db.maplemonk.amazonads_overall_marketing ly on ly.date::date = dateadd(year,-1, a.date)::date and a.asin_new =ly.asin_new group by 1,2,3,4,5,6,7 order by 1 desc ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        