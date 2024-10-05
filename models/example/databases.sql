{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.zouk_db_target_vs_Achieved as With Sales as ( Select * ,case when CURRENT_DATE() > Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY)+1 end as days_Remaining ,EXTRACT(DAY FROM LAST_DAY(cast(Month as date))) as no_of_days ,safe_divide(sales,(EXTRACT(DAY FROM cast(Month as date)) -(case when CURRENT_DATE() >Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY) end) )-1) Avg_Sales from ( Select marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,PRODUCT_CATEGORY ,COLLECTION ,last_day(Order_Date) Month ,sum(ifnull(SELLING_PRICE,0)) Sales from maplemonk.zouk_sales_consolidated group by 1,2,3,4,5,6 ) ), Targets as ( select marketplace ,last_day(PARSE_DATE(\'%b/%Y\', month)) Month ,category ,collection ,1 as flag ,ifnull(cast(replace(target,\',\',\'\') as int64),0) Targets from MapleMonk.zouk_db_Metrics_Targets where category is not null ), Targets_non_category as ( select marketplace ,last_day(PARSE_DATE(\'%b/%Y\', month)) Month ,\'1\' as category ,\'1\' as collection ,1 as flag ,ifnull(cast(replace(target,\',\',\'\') as int64),0) Targets from MapleMonk.zouk_db_Metrics_Targets where category is null ), category_targets as ( select coalesce(s.marketplace,t.marketplace) marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,coalesce(s.PRODUCT_CATEGORY,t.category) PRODUCT_CATEGORY ,coalesce(s.collection,t.collection) collection , cast (coalesce(s.Month,t.month) as date) month ,Sales ,days_Remaining ,no_of_days ,Avg_Sales ,T.Targets,T.flag from Sales S full outer join Targets T on lower(s.marketplace) = lower(T.marketplace) and s.month = t.month and lower(S.product_category) = lower(T.category) ), Sales_non_category as ( Select * ,case when CURRENT_DATE() > Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY)+1 end as days_Remaining ,EXTRACT(DAY FROM LAST_DAY(cast(Month as date))) as no_of_days ,safe_divide(sales,(EXTRACT(DAY FROM cast(Month as date)) -(case when CURRENT_DATE() >Month then 0 else DATE_DIFF(Month, CURRENT_DATE(), DAY) end) )-1) Avg_Sales from ( select Marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,Month ,\'\' as PRODUCT_CATEGORY ,\'\' as collection ,sum(ifnull(Sales,0)) Sales from category_targets where ifnull(flag,0) != 1 group by 1,2,3,4 ) ), non_Category_targets as ( select coalesce(s.marketplace,t.marketplace) marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,\'\' as PRODUCT_CATEGORY ,\'\' as collection , cast (coalesce(s.Month,t.month) as date) month ,Sales ,days_Remaining ,no_of_days ,Avg_Sales ,T.Targets,T.flag from Sales_non_category S full outer join Targets_non_category T on lower(s.marketplace) = lower(T.marketplace) and s.month = t.month ) (select upper(marketplace) marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,PRODUCT_CATEGORY ,collection ,cast (Month as date) month ,Sales ,days_Remaining ,no_of_days ,Avg_Sales ,Targets ,flag from category_targets where ifnull(flag,0) = 1) union all (select upper(marketplace) marketplace ,FINAL_MARKETPLACE ,MARKETPLACE_SEGMENT ,PRODUCT_CATEGORY ,collection ,cast (Month as date) month ,Sales ,days_Remaining ,no_of_days ,Avg_Sales ,Targets ,flag from non_Category_targets )",
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
            