{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.zouk_db_target_vs_Achieved as With Sales as ( Select * ,case when CURRENT_DATE() > Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY)+1 end as days_Remaining ,EXTRACT(DAY FROM LAST_DAY(cast(Month as date))) as no_of_days ,safe_divide(sales,(EXTRACT(DAY FROM cast(Month as date)) -(case when CURRENT_DATE() >Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY) end) )-1) Avg_Sales from ( Select marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,last_day(Order_Date) Month ,cast(sum(ifnull(SELLING_PRICE,0)) as int64) Sales from maplemonk.zouk_sales_consolidated where lower(ifnull(order_status,\'\')) not in (\'cancelled\') group by 1,2,3,4 ) ) , Targets as ( select upper(marketplace) marketplace ,last_day(PARSE_DATE(\'%b/%Y\', month)) Month ,sum(ifnull(cast(replace(target,\',\',\'\') as int64),0)) Targets from MapleMonk.zouk_db_Metrics_Targets group by 1,2 ) select s.*,T.Targets from Sales S left join Targets T on lower(s.marketplace) = lower(T.marketplace) and s.month = t.month",
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
            