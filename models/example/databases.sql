{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.MAPLEMONK.XYXX_DB_AVP_FactItems as with sales as ( select STARTDATE Date ,\'AMAZON_VENDOR_CENTRAL\' MARKETPLACE ,ASIN ,SHIPPEDCOGS:\"amount\"::float Shipped_COGS ,SHIPPEDCOGS:\"currencyCode\" Shipped_COGS_Currency_Code ,ifnull(orderedunits,0)::float Ordered_Units ,ifnull(shippedunits,0)::float Shipped_Units ,ifnull(orderedrevenue:\"amount\",0)::float Ordered_Revenue ,ifnull(customerreturns,0)::float Customer_Returns ,SHIPPEDREVENUE:\"amount\"::float Shipped_Revenue ,SHIPPEDREVENUE:\"currencyCode\" Shipped_Revenue_Currency_Code ,\'MANUFACTURING_RETAIL\' Data_Stream from XYXX_DB.MAPLEMONK.avp_XYXX_get_vendor_sales_report ), sessions as ( select startdate Date ,\'AMAZON_VENDOR_CENTRAL\' MARKETPLACE ,ASIN ,ifnull(GLANCEVIEWS,0) GLANCEVIEWS ,\'MANUFACTURING_RETAIL\' Data_Stream from XYXX_DB.MAPLEMONK.avp_XYXX_get_vendor_traffic_report ), SKUMASTER AS ( select * from ( select ASIN ,ACCOUNT ,UPPER(CATEGORY) CATEGORY ,SELLER_SKU ,RANGE ,PARENT_SKU ,COMMONSKU ,row_number() over (partition by ASIN order by 1) rw from XYXX_DB.MAPLEMONK.ASIN_MAPPING ) where rw=1 ) select coalesce(sales.Date, sessions.Date) Date ,coalesce(sales.marketplace, sessions.marketplace) marketplace ,coalesce(sales.ASIN, sessions.ASIN) ASIN ,SKUMASTER.COMMONSKU SKU ,SKUMASTER.ACCOUNT ,null Product_Name_Final ,SKUMASTER.category Mapped_Category ,SKUMASTER.RANGE Mapped_Sub_Category ,ifnull(SALES.Shipped_COGS,0) Shipped_COGS ,SALES.Shipped_COGS_Currency_Code ,ifnull(SALES.Ordered_Units,0) Ordered_Units ,ifnull(SALES.Shipped_Units,0) Shipped_Units ,ifnull(SALES.Ordered_Revenue,0) Ordered_Revenue ,ifnull(SALES.Customer_Returns,0) Customer_Returns ,ifnull(SALES.Shipped_Revenue,0) Shipped_Revenue ,SALES.Shipped_Revenue_Currency_Code ,coalesce(SALES.Data_Stream, Sessions.Data_stream) Data_stream ,ifnull(SESSIONS.GLANCEVIEWS,0) Glance_views ,ifnull(SALES.Ordered_Revenue,0) Shipped_Revenue_Final ,ifnull(SALES.Customer_Returns,0) Returned_Revenue_Final from sales full outer join sessions on Sales.date=sessions.date and sales.ASIN=sessions.ASIN and lower(Sales.marketplace)=lower(sessions.marketplace) left join SKUMASTER on coalesce(sales.ASIN, sessions.ASIN) = SKUMASTER.ASIN;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        