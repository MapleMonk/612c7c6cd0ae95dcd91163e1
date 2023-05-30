{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table buildskill148_DB.MAPLEMONK.Buildskill_AVP_FactItems as with sales as ( select STARTDATE Date ,ASIN ,SHIPPEDCOGS:\"amount\" Shipped_COGS ,replace(SHIPPEDCOGS:\"currencyCode\",\'\"\',\'\') Shipped_COGS_Currency_Code ,ifnull(orderedunits,0) Ordered_Units ,ifnull(shippedunits,0) Shipped_Units ,case when orderedrevenue is null then 0 else orderedrevenue:\"amount\" end::float Ordered_Revenue ,ifnull(customerreturns,0) Customer_Returns ,SHIPPEDREVENUE:\"amount\" Shipped_Revenue ,customer_returns*(div0(Ordered_Revenue,ordered_units)) returned_revenue ,replace(SHIPPEDREVENUE:\"currencyCode\",\'\"\',\'\') Shipped_Revenue_Currency_Code ,\'Manufacturing_Retail\' Data_Stream from buildskill148_DB.maplemonk.manufacturing_get_vendor_sales_report ), sessions as ( select startdate Date ,ASIN ,ifnull(GLANCEVIEWS,0) GLANCEVIEWS ,\'Sourcing_Retail\' Data_Stream from buildskill148_DB.maplemonk.sourcing_get_vendor_traffic_report ), SKUMASTER AS ( select * from (select asin ,null product_id ,product_title name ,category ,sub_category ,row_number() over (partition by asin order by asin) rw from buildskill148_db.MapleMonk.sku_master ) where rw=1 ) select coalesce(sales.Date, sessions.Date) Date ,coalesce(sales.ASIN, sessions.ASIN) ASIN ,SKUMASTER.name Product_Name_Final ,SKUMASTER.category Mapped_Category ,SKUMASTER.sub_category Mapped_Sub_Category ,ifnull(SALES.Shipped_COGS,0) Shipped_COGS ,SALES.Shipped_COGS_Currency_Code ,ifnull(SALES.Ordered_Units,0) Ordered_Units ,ifnull(SALES.Shipped_Units,0) Shipped_Units ,ifnull(SALES.Ordered_Revenue,0) Ordered_Revenue ,ifnull(SALES.Customer_Returns,0) Customer_Returns ,ifnull(SALES.Shipped_Revenue,0) Shipped_Revenue ,ifnull(SALES.returned_Revenue,0) Returned_Revenue ,SALES.Shipped_Revenue_Currency_Code ,coalesce(SALES.Data_Stream, Sessions.Data_stream) Data_stream ,ifnull(SESSIONS.GLANCEVIEWS,0) Glance_views ,c.\"Amazon Landing Cost (Pre-GST)\" cost from sales full outer join sessions on Sales.date=sessions.date and sales.ASIN=sessions.ASIN left join SKUMASTER on coalesce(sales.ASIN, sessions.ASIN) = SKUMASTER.asin left join buildskill148_db.maplemonk.google_sheets_costs c on sales.asin = c.asin ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BuildSkill148_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        