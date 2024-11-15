{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or Replace table maplemonk.zouk_inwards_actual_vs_target_MTD as WITH TARGETS as( WITH parsed_dates AS ( SELECT PARSE_DATE(\'%d-%b-%y\', Start_Date) AS Start_Date, PARSE_DATE(\'%d-%b-%y\', End_Date) AS End_Date, CAST(Target AS FLOAT64) AS Target, POC, Upper(Vendor) as Vendor, Upper(Category) as Category FROM maplemonk.inward_target ), date_series AS ( SELECT Start_Date, End_Date, Target, POC, Vendor, Category, DATE_ADD(Start_Date, INTERVAL day_offset DAY) AS daily_date FROM parsed_dates, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(End_Date, Start_Date, DAY))) AS day_offset ) SELECT daily_date AS Date, Target / (DATE_DIFF(End_Date, Start_Date, DAY) + 1) AS Daily_Target, POC, Vendor, Category FROM date_series ORDER BY Date ), inwards as ( select PARSE_DATE(\'%m/%d/%Y\', Date) Date, Upper(vendor) As Vendor, Upper(category) as Category, sum(safe_cast(Inward_Quantity as int64)) as Inward_Quantity, from maplemonk.inward_data fi left join (select * from (select marketplace_sku skucode, name, category, sub_category, category_code, collection, print, PRODUCT_TYPE, commonsku, BAU_OFFLINE, BAU_ONLINE, TAX_RATE , row_number() over (partition by commonsku order by 1) rw from zouk-wh.maplemonk.final_sku_master ) where rw = 1 )pid on lower(fi.sku_id) = lower(pid.commonsku) where date is not null and date != \'#REF!\' group by 1,2,3 ) Select coalesce(inw.date,tar.date) as date, Upper(coalesce(inw.vendor,tar.vendor)) as vendor, Upper(coalesce(inw.category,tar.category)) as category, tar.poc, ifnull(inw.Inward_Quantity,0) Inward_Quantity, ifnull(tar.Daily_Target,0) targets from inwards inw full outer join TARGETS tar on lower(inw.Category) = lower(tar.Category) and cast(inw.Date as date) = cast(tar.date as date) and lower(inw.Vendor) = lower(tar.Vendor) ;",
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
            