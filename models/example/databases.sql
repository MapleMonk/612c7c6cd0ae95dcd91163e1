{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table PERFORA_DB.MAPLEMONK.Perfora_AVP_FactItems as with sales as ( select STARTDATE Date ,ASIN ,SHIPPEDCOGS:\"amount\" Shipped_COGS ,SHIPPEDCOGS:\"currencyCode\" Shipped_COGS_Currency_Code ,ifnull(orderedunits,0) Ordered_Units ,ifnull(shippedunits,0) Shipped_Units ,ifnull(orderedrevenue,0) Ordered_Revenue ,ifnull(customerreturns,0) Customer_Returns ,SHIPPEDREVENUE:\"amount\" Shipped_Revenue ,SHIPPEDREVENUE:\"currencyCode\" Shipped_Revenue_Currency_Code ,\'Sourcing_Retail\' Data_Stream from perfora_db.maplemonk.perfora_avp_get_vendor_sales_report ), sessions as ( select startdate Date ,ASIN ,ifnull(GLANCEVIEWS,0) GLANCEVIEWS ,\'Sourcing_Retail\' Data_Stream from perfora_db.maplemonk.perfora_avp_get_vendor_traffic_report ), selling_price as ( select * from (select ASIN ,SKU ,try_to_date(\"Start Date (mm/dd/yyyy)\") Start_Date ,try_to_date(\"End Date\") End_date ,ifnull(try_to_double(\"Selling Price\"),0) selling_price ,row_number() over (partition by ASIN, \"Start Date (mm/dd/yyyy)\" order by \"Start Date (mm/dd/yyyy)\" desc) rw from perfora_db.maplemonk.perfora_gs_vendor_central_selling_price ) where rw=1 ) select coalesce(sales.Date, sessions.Date) Date ,coalesce(sales.ASIN, sessions.ASIN) ASIN ,ifnull(SALES.Shipped_COGS,0) Shipped_COGS ,SALES.Shipped_COGS_Currency_Code ,ifnull(SALES.Ordered_Units,0) Ordered_Units ,ifnull(SALES.Shipped_Units,0) Shipped_Units ,ifnull(SALES.Ordered_Revenue,0) Ordered_Revenue ,ifnull(SALES.Customer_Returns,0) Customer_Returns ,ifnull(SALES.Shipped_Revenue,0) Shipped_Revenue ,SALES.Shipped_Revenue_Currency_Code ,coalesce(SALES.Data_Stream, Sessions.Data_stream) Data_stream ,ifnull(SESSIONS.GLANCEVIEWS,0) Glance_views ,ifnull(sp.selling_price,0) Selling_Price ,ifnull(SALES.Shipped_Units,0)*ifnull(sp.selling_price,0) Shipped_Revenue_Final ,ifnull(SALES.Customer_Returns,0)*ifnull(sp.selling_price,0) Returned_Revenue_Final from sales full outer join sessions on Sales.date=sessions.date and sales.ASIN=sessions.ASIN left join selling_price sp on coalesce(sales.ASIN, sessions.ASIN) = sp.ASIN and coalesce(sales.Date, sessions.Date) >= sp.start_date and coalesce(sales.Date, sessions.Date) <= sp.end_date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        