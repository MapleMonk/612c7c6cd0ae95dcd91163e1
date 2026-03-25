{{ config(
            materialized='table',
                post_hook={
                    "sql": "select account_name, sum(spend) from eatanytime-wh-484310.maplemonk.eatanytime_wh_484310_MARKETING_CONSOLIDATED where date >= \'2026-02-01\' group by 1 select platform, sum(spend) from eatanytime-wh-484310.maplemonk.eatanytime_wh_484310_MARKETING_summary where date >= \'2026-02-01\' group by 1 create or replace table eatanytime-wh-484310.maplemonk.eatanytime_wh_484310_MARKETING_summary as with marketing_Data as ( select date, case when account_name in (\'GOOGLE ADS\', \'FACEBOOK\') then \'WEBSITE\' when account_name like (\'%FLIPKART%\') then \'FLIPKART\' else account_name end marketing_sales_channel, sum(spend) spend, sum(conversion_value) ad_sales, from eatanytime-wh-484310.maplemonk.eatanytime_wh_484310_MARKETING_CONSOLIDATED group by 1,2 ), sales_data as ( select cast(ordeR_date as date) order_Date, case when marketplace like \'%FLIPKART%\' then \'FLIPKART\' when marketplace like \'%AMAZON%\' then \'AMAZON\' when marketplace like \'%ZEPTO%\' then \'ZEPTO\' when marketplace like \'%BLINKIT%\' then \'BLINKIT\' when marketplace like \'%SWIGGY%\' then \'SWIGGY\' when marketplace like \'%WEBSITE%\' and upper(sales_channel) = \'WEBSITE\' then \'WEBSITE\' end as marketplace_mapped, sum(selling_price) total_sales from eatanytime-wh-484310.maplemonk.eatanytime_wh_484310_sales_consolidated where marketplace in (\'ZEPTO\', \'SWIGGY\', \'WEBSITE\', \'AMAZON\', \'FLIPKART\', \'FLIPKART FBF\', \'FLIPKART ADVANTAGE\') group by 1,2 ) select coalesce(a.date, b.order_Date) date, coalesce(upper(a.marketing_sales_channel), upper(b.marketplace_mapped)) platform, spend, ad_sales, total_sales from marketing_data a full outer join sales_Data b on a.date = b.ordeR_Date and upper(a.marketing_sales_channel) = upper(b.marketplace_mapped)",
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
            