{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table welspun_db.maplemonk.aa_us_marketing_summary_asin_welspun as with ctea as ( select purchase_date::date as date ,asin ,sum(item_tax) ITEM_TAX ,ifnull(sum(item_price),0) as item_price ,ifnull(sum(gift_wrap_price),0) as gift_wrap_price ,ifnull(sum(item_promotion_discount),0) as item_promotion_discount ,sum(quantity) as quantity ,count(distinct amazon_order_id) orders ,sum(return_quantity) as return_quantity ,sum(return_value) as return_value ,sum(replacement_quantity) as replacement_quantity from welspun_db.maplemonk.asp_consolidated_welspun where lower(sales_channel)=\'amazon.com\' group by 1,2) , cteb as ( select date ,asin ,sum(orders) orders ,sum(spend) spend ,sum(sessions) sessions ,sum(ad_sales) Ad_Sales ,sum(conversions) Ad_Orders from welspun_db.maplemonk.aa_us_marketing where profileid=\'2659756818745852\' group by 1,2 ) select coalesce(a.date,b.date) as Date ,coalesce(a.asin,b.asin) as ASIN ,a.item_tax ,a.item_price ,a.item_promotion_discount ,a.gift_wrap_price ,a.quantity ,a.orders ,a.return_quantity ,a.return_value ,a.replacement_quantity ,b.spend ,b.sessions ,b.Ad_Sales ,b.Ad_Orders from ctea a full outer join cteb b on a.date =b.date and a.asin =b.asin; create or replace table welspun_db.maplemonk.aa_us_marketing_summary_welspun as with ctea as ( select purchase_date::date as date ,sum(item_tax) item_tax ,ifnull(sum(item_price),0) as item_price ,ifnull(sum(gift_wrap_price),0) as gift_wrap_price ,ifnull(sum(item_promotion_discount),0) as item_promotion_discount ,sum(quantity) as quantity ,count(distinct amazon_order_id) orders ,sum(return_quantity) as return_quantity ,sum(return_value) as return_value ,sum(replacement_quantity) as replacement_quantity from welspun_db.maplemonk.asp_consolidated_welspun where lower(sales_channel)=\'%amazon.com%\' group by 1) , cteb as ( select date ,sum(orders) orders ,sum(spend) spend ,sum(sessions) sessions ,sum(AD_SALES) Ad_sales from welspun_db.maplemonk.aa_us_marketing where profileid=\'2659756818745852\' group by 1 ) select coalesce(a.date,b.date) as Date ,a.item_tax ,a.item_price ,a.item_promotion_discount ,a.gift_wrap_price ,a.quantity ,a.orders ,a.return_quantity ,a.return_value ,a.replacement_quantity ,b.spend ,b.sessions ,b.Ad_sales from ctea a full outer join cteb b on a.date =b.date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from WELSPUN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        