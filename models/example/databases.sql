{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table prd_db.Beardo.dwh_Sales_Cost_Source_intermediate as select * from ( with returnsales as ( select order_date::date order_date ,marketplace ,marketing_channel ,sum(total_return_amount) TOTAL_RETURN_AMOUNT ,sum(total_return_amount_excl_tax) TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum(total_returned_quantity) TOTAL_RETURNED_QUANTITY from prd_db.Beardo.DWH_UNICOMMERCE_RETURNS_SUMMARY_BEARDO group by 1,2,3 order by 1 desc ), orders as ( select FI.order_date::date Date ,FI.SHOP_NAME Marketplace ,FI.CHANNEL Marketing_Channel ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,count(distinct FI.order_id) Total_Orders ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,ifnull(sum(FI.discount),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.discount end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') then FI.discount end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(order_status) in (\'delivered\') then order_id end) Delivered_Orders ,count(distinct case when lower(order_status) in (\'returned\',\'rto\') then order_id end) Returned_Orders ,count(distinct case when dispatch_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,count(distinct case when lower(order_status) in (\'shipped\',\'printed\',\'confirmed\',\'returned\',\'ready to dispatch\') then order_id end) Realised_Orders ,ifnull(sum(case when lower(order_status) in (\'delivered\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Delivered_Revenue ,ifnull(sum(case when lower(order_status) in (\'returned\',\'rto\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Returned_Revenue ,ifnull(sum(case when dispatch_date is not null and lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0) end),0) Dispatched_Revenue ,ifnull(sum(case when lower(order_status) in (\'shipped\',\'printed\',\'confirmed\',\'returned\',\'ready to dispatch\') then ifnull(FI.SELLING_PRICE,0) end),0) Realised_Revenue ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-6,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date)>=dateadd(month,-3,date_trunc(\'month\',order_date)) and date_trunc(\'month\',acquisition_date)<date_trunc(\'month\',order_date) then customer_id_final end) L12M_Customers_Retained ,sum(case when lower(FI.new_customer_flag) = \'repeat\' then ifnull(FI.selling_price,0) end) Repeat_Customer_Revenue ,count(distinct case when lower(FI.payment_mode) in (\'cod\') then FI.order_id end) as COD_Orders from prd_db.Beardo.dwh_SALES_CONSOLIDATED FI group by 1,2,3 ), spendd2c as (select date ,case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'Shopify_beardo\' when lower(channel) like any (\'%amazon%\') then \'AMAZON\' when lower(channel) like any (\'%flipkart%\') then \'Flipkart\' else \'Others\' end as Marketplace ,channel as Marketing_Channel ,sum(spend) as spend from prd_db.Beardo.dwh_MARKETING_CONSOLIDATED group by 1,2,3 ) select coalesce(fi.Date,MC.date, RS.order_date) as date, Upper(coalesce(fi.marketplace, MC.marketplace, Rs.marketplace)) as Marketplace, Upper(coalesce(fi.marketing_channel, MC.marketing_channel, RS.marketing_channel)) as Marketing_Channel, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, RS.TOTAL_RETURNED_QUANTITY as Return_Quantity, RS.TOTAL_RETURN_AMOUNT as Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, Realised_Revenue, spend as marketing_spend, Repeat_Customer_Revenue, COD_Orders from orders FI full outer join spendd2c MC on FI.Date = MC.date and lower(FI.Marketplace)=(MC.Marketplace) and lower(FI.Marketing_Channel)=(MC.Marketing_Channel) full outer join returnsales RS on RS.ordeR_Date = FI.date and lower(RS.marketplace)=lower(FI.marketplace) and lower(FI.Marketing_Channel)=(RS.Marketing_Channel) ) where marketplace not in (\'AMAZON\') ; Create or replace table prd_db.Beardo.dwh_SALES_COST_SOURCE as select coalesce(a.date, b.date) as date, upper(coalesce(a.marketplace,b.marketplace)) as Marketplace, upper(coalesce(a.marketing_channel,b.marketing_channel)) as Marketing_Channel, Total_Sales, TOTAL_SALES_EXCL_CANCL, Total_Orders, Orders_EXCL_CANCL, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, Realised_Revenue, marketing_spend, Repeat_Customer_Revenue, COD_Orders, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date from prd_db.Beardo.dwh_Sales_Cost_Source_intermediate a full outer join (select date ,Marketplace ,Marketing_Channel ,sum(gross_sales) over (partition by date order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by date order by date asc rows between unbounded preceding and current row) customers from ( select B.date, B.shop_name Marketplace, B.Source Marketing_Channel, sum(ifnull(selling_price,0)) gross_sales, count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from prd_db.Beardo.DWH_SALES_CONSOLIDATED A full outer join ( select * from (select distinct order_date::date date from prd_db.Beardo.dwh_SALES_CONSOLIDATED X) cross join (select distinct shop_name, source from prd_db.Beardo.dwh_SALES_CONSOLIDATED) Y) B on A.order_date::date=B.date group by B.date, B.shop_name, B.Source order by B.date desc ) order by date desc ) b on a.Date = b.date and lower(a.Marketplace)=lower(b.Marketplace) and lower(a.Marketing_Channel) = lower(b.Marketing_channel) order by 1 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from PRD_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        