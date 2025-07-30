{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ras-wh.maplemonk.Moody_Nykaa_DSR_Fact_Items as select \'MOODY\' as Brand, \'NYKAA\' as marketplace, FORMAT_DATE(\'%Y-%m-%d\',PARSE_DATE(\'%d-%b-%y\', Date)) AS Date, Day, SALE_EVENT_NAME as Sale_Event_Name, Discount__ as Discount_Percentage, ADDITIONAL_DISCOUNT___CRT_RULES as Additional_Discount_CRT_Rules, cast(replace(UNITS,\',\',\'\') as float64) as Units, cast(replace(ORDERS,\',\',\'\') as float64) as Orders, cast(replace(AOV,\',\',\'\') as float64) as AOV, cast(ABS as float64) as ABS, cast(replace(ASP,\',\',\'\') as float64) as ASP, cast(replace(MRP_SALE___IN_RS_,\',\',\'\') as float64) as MRP_Sales_in_INR, cast(replace(SP_SALE___IN_RS_,\',\',\'\') as float64) as SP_Sales_in_INR, cast(replace(replace(Target_MRP_DRR,\',\',\'\'),\'₹\',\'\') as float64) as Target_MRP_DRR, cast(replace(replace(Target_SP_DRR,\',\',\'\'),\'₹\',\'\') as float64) as Target_SP_DRR, case when daily_DRR_achvmnt__ in (\'1\',\'2\',\'0\') then cast(replace(daily_DRR_achvmnt__,\',\',\'\') as float64) else null end as Daily_DRR_Achievement_Percent, cast(replace(DISCOUNT_DEBIT_IN_RS,\',\',\'\') as float64) as Discount_Debit_in_INR, cast(replace(Discount___to_MRP,\',\',\'\')as float64) as Discount_Percent_to_MRP, cast(replace(Discount___to_Net_Sales,\',\',\'\')as float64) as Discount_Percent_to_Net_Sales, safe_cast(replace(TILES_VISIBILTY_BANNERS,\',\',\'\')as float64) as Tiles_Visibility_Banners, TITLE___VISIBILTY_Describe as Tiles_Visibility_Description, cast(replace(Curation,\',\',\'\')as float64) as Curation, CURATION__Describe_ as Curation_Description, cast(replace(OFFSITE_SPENDS,\',\',\'\')as float64) as Offsite_Spends, safe_cast(replace(PLA,\',\',\'\')as float64) as PLA, cast(replace(replace(TOTAL_MARKETING_SPEND,\',\',\'\'),\'₹\',\'\') as float64) as Total_Marketing_Spends, cast(replace(replace(TOTAL_MARKETING_SPEND_Incl__Taxes,\',\',\'\'),\'₹\',\'\') as float64) as Total_Marketing_Spends_Incl_Tax, cast(replace(replace(TOTALINVESTMENT___MARKETING___DISCOUNT_SPEND__PLA,\',\',\'\'),\'₹\',\'\') as float64) as Total_Investment_Marketing_Discount, cast(ROI as float64) as ROI, Discount_Ticket_ID, cast(replace(replace(MTD_Target,\',\',\'\'),\'₹\',\'\') as float64) as MTD_Target, cast((cast(replace(rc.return__,\'.00%\',\'\') as int64)/100) as float64) as return_perc, cast((cast(replace(rc.comission__,\'.00%\',\'\') as int64)/100) as float64) as commission_perc from `maplemonk.Google_Sheets_Nykaa_Moody_DSR` nm left join `MAPLEMONK.Returns_and_commissions_MKT_return_` rc on replace(lower(rc.mkt_place),\' \',\'\') = \'nykaa\' where DATE like \'%-%\';",
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
            