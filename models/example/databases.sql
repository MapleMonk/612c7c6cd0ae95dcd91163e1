{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ras_db.MAPLEMONK.RAS_AVP_FactItems as with sales as ( select STARTDATE Date ,\'AMAZON_VENDOR_CENTRAL_RK_WORLD\' MARKETPLACE ,ASIN ,SHIPPEDCOGS:\"amount\" Shipped_COGS ,SHIPPEDCOGS:\"currencyCode\" Shipped_COGS_Currency_Code ,ifnull(orderedunits,0) Ordered_Units ,ifnull(shippedunits,0) Shipped_Units ,ifnull(orderedrevenue:\"amount\",0) Ordered_Revenue ,ifnull(customerreturns,0) Customer_Returns ,SHIPPEDREVENUE:\"amount\" Shipped_Revenue ,SHIPPEDREVENUE:\"currencyCode\" Shipped_Revenue_Currency_Code ,\'MANUFACTURING_RETAIL\' Data_Stream from ras_db.maplemonk.ras_get_vendor_sales_report ), sessions as ( select startdate Date ,\'AMAZON_VENDOR_CENTRAL_RK_WORLD\' MARKETPLACE ,ASIN ,ifnull(GLANCEVIEWS,0) GLANCEVIEWS ,\'MANUFACTURING_RETAIL\' Data_Stream from ras_db.maplemonk.ras_get_vendor_traffic_report ) select coalesce(sales.Date, sessions.Date) Date ,coalesce(sales.marketplace, sessions.marketplace) marketplace ,coalesce(sales.ASIN, sessions.ASIN) ASIN ,p.name product_name_final ,p.category product_category ,p.sub_category product_sub_category ,ifnull(SALES.Shipped_COGS,0) Shipped_COGS ,SALES.Shipped_COGS_Currency_Code ,ifnull(SALES.Ordered_Units,0) Ordered_Units ,ifnull(SALES.Shipped_Units,0) Shipped_Units ,ifnull(SALES.Ordered_Revenue,0) Ordered_Revenue ,ifnull(SALES.Customer_Returns,0) Customer_Returns ,ifnull(SALES.Shipped_Revenue,0) Shipped_Revenue ,SALES.Shipped_Revenue_Currency_Code ,coalesce(SALES.Data_Stream, Sessions.Data_stream) Data_stream ,ifnull(SESSIONS.GLANCEVIEWS,0) Glance_views ,ifnull(SALES.Ordered_Revenue,0) Shipped_Revenue_Final from sales full outer join sessions on Sales.date=sessions.date and sales.ASIN=sessions.ASIN and lower(Sales.marketplace)=lower(sessions.marketplace) left join (select * from (select asin, category, \"Sub-Category\" sub_category, \"Product Variant Name\" name, row_number() over (partition by asin order by 1) rw from ras_db.maplemonk.sku_master_ras_copy ) where rw=1 )p on sales.asin = p.asin",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ras_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        