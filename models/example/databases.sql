{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.Amazon_Last_Year_Amazonads_Vahdam as with daily_asin_sales as (select region ,date ,asin_new,brand ,sum(ORDERS) as Orders ,sum(QUANTITY) as Quantity ,sum(SALES_USD) as Sales_usd ,sum(SPEND) as Spend ,sum(SESSIONS) as Sessions from vahdam_db.maplemonk.amazonads_overall_marketing group by 1,2,3,4) select a.date::date as Date ,ly.date::date as Last_year_date ,a.asin_new ,a.brand ,a.region ,sum(a.ORDERS) as Orders ,sum(a.QUANTITY) as Quantity ,sum(a.SALES_USD) as Sales ,sum(a.SPEND) as Spend ,sum(a.SESSIONS) as Sessions ,sum(ly.ORDERS) as LY_Orders ,sum(ly.QUANTITY) as LY_Quantity ,sum(ly.SALES_USD) as LY_Sales ,sum(ly.SPEND) as LY_Spend ,sum(ly.SESSIONS) as LY_Sessions From daily_asin_sales a left join (daily_asin_sales) ly on ly.date::date = dateadd(year,-1, a.date)::date and lower(a.asin_new) =lower(ly.asin_new) group by 1,2,3,4,5 order by 1 desc;",
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
                        