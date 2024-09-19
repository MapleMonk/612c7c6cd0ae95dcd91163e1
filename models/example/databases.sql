{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_PRODUCT_SUMMARY_THREE60YOU as with invoicedatemetrics as ( select try_to_date(FI.invoice_date) Invoice_Date ,upper(FI.final_channel) Channel ,upper(FI.SHOP_NAME) SHOP_NAME ,upper(data_source) data_source ,upper(trim(product_name_mapped)) product ,upper(trim(category)) category ,count(distinct case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then order_id end ) rr_pre_invoice_Delivered_Orders ,sum(case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then ifnull(selling_price,0) - ifnull(tax,0) end ) rr_Realised_Revenue ,count(distinct case when rr_flag is null and lower(order_status) not in (\'cancelled\') then order_id end ) wo_rr_pre_invoice_Delivered_Orders ,sum(case when rr_flag is null and lower(order_status) not in (\'cancelled\') then ifnull(selling_price,0) - ifnull(tax,0) end ) wo_rr_Realised_Revenue ,count(distinct case when lower(order_status) not in (\'cancelled\') then reference_code end) pre_invoice_Delivered_Orders ,sum(ifnull((case when lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0)) Realised_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI where not(lower(order_status) in (\'cancelled\') ) and invoice_date != \'\' group by 1,2,3,4,5,6 order by 1 desc ), returnsales as ( select return_date::date return_date ,upper(channel) AS channel ,upper(shop_name) shop_name ,upper(data_source) data_source ,trim(product_name_mapped) as product ,trim(category) as CATEGORY ,count(distinct case when rr_flag = 1 then reference_code end ) rr_return_orders_count ,count(distinct case when rr_flag is null then reference_code end ) wo_rr_return_orders_count ,sum( case when rr_flag = 1 then ifnull(total_return_amount_excl_tax,0) end) rr_TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum( case when rr_flag is null then ifnull(total_return_amount_excl_tax,0) end) wo_rr_TOTAL_RETURN_AMOUNT_EXCL_TAX ,count(distinct reference_code) as return_orders_count ,sum(total_return_amount) TOTAL_RETURN_AMOUNT ,sum(total_return_amount_excl_tax) TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum(total_returned_quantity) TOTAL_RETURNED_QUANTITY from RPSG_DB.MAPLEMONK.easyecom_returns_summary_three60 group by 1,2,3,4,5,6 order by 1 desc ) , orders as ( select FI.order_date::date Date ,upper(FI.final_channel) Channel ,upper(FI.SHOP_NAME) SHOP_NAME ,upper(trim(product_name_mapped)) as product ,upper(trim(category)) as category ,upper(data_source) as data_source ,count(distinct case when lower(payment_mode) = \'cod\' then order_id end) cod_Orders ,count(distinct case when lower(payment_mode) = \'prepaid\' then order_id end) prepaid_Orders ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales ,sum( case when rr_flag is null then ifnull(FI.SELLING_PRICE,0) end) wo_rr_TOTAL_Sales ,count(distinct appointment_id) as total_consultations ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,ifnull(sum(case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\')) then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL_RTO ,count(distinct case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\'))then order_id end ) as Orders_EXCL_CANCL_RTO ,sum(ifnull(selling_price,0)) - sum(ifnull(tax,0)) as Total_Sales_Ex_Tax ,count(distinct case when rr_flag = 1 then FI.order_id end ) Total_RR_Orders ,sum(case when rr_flag = 1 then ifnull(selling_price,0) - ifnull(tax,0) end ) RR_Booked_Rev_After_Tax ,count(distinct FI.order_id) Total_Orders ,count(distinct case when rr_flag is null then FI.order_id end ) Total_wo_RR_Orders ,sum(case when rr_flag is null then ifnull(selling_price,0) - ifnull(tax,0) end ) wo_RR_Booked_Rev_After_Tax ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.order_id end)) as Repeat_Orders ,sum(case when lower(FI.new_customer_flag) = \'repeat\' then ifnull(FI.selling_price,0) end) as Repeat_Revenue ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then Fi.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.order_id end))) Repeat_orders_EXCL_CANCL ,ifnull(sum(FI.discount_mrp),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount_mrp),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.discount_mrp end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') then FI.discount_mrp end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when FI.return_flag=1 then order_id end )as Return_Orders ,count(distinct case when lower(order_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(final_status) in (\'delivered\') then order_id end) Delivered_Orders ,count(distinct case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then order_id end) Returned_Orders ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,Orders_EXCL_CANCL - Returned_Orders as Realised_Orders ,ifnull(sum(case when lower(final_status) in (\'delivered\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Delivered_Revenue ,ifnull(sum(case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Returned_Revenue ,ifnull(sum(case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0) end),0) Dispatched_Revenue ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-6,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L12M_Customers_Retained ,sum(case when lower(FI.new_customer_flag_month) = \'repeat\' then ifnull(FI.selling_price,0) end) Repeat_Customer_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI group by 1,2,3,4,5,6 ), spend as ( select Date, upper(channel) channel, Upper(account) as shop_name ,upper(data_source) data_source ,trim(product) product ,null as category ,sum(spend)Spend ,SUM(CASE WHEN RR_FLAG = 1 THEN spend ELSE 0 END) AS RR_SPENDS ,SUM(CASE WHEN RR_FLAG is null THEN spend ELSE 0 END) AS wo_RR_SPENDS from rpsg_db.MAPLEMONK.marketing_consolidated_three60you group by 1,2,3,4,5 order by 1 desc ), consultations as ( SELECT end_timestamp::date AS start_date, \'CONSULTATIONS\' AS c_marketplace, \'THREE60\' as data_source, null as c_channel, null as product, null as category, count(distinct case when LOWER(status) = \'completed\' then id end) as consultation_completed, count(distinct id ) as consultation_confirmed FROM ( select * from ( select a.*, case when LOWER(status) = \'completed\' then 1 else 2 end as df, right(regexp_replace(c.phone, \'[^a-zA-Z0-9]+\'),10) phone, row_number() over(partition by end_timestamp::date,phone order by df asc ) rw from rpsg_db.maplemonk.pg_three60you_appointment a left join rpsg_db.maplemonk.pg_three60you_customer c on lower(c.id) = lower(a.customer_id) where right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) not in (select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) from RPSG_DB.maplemonk.three60you_test_consultations ) )where rw=1 ) group by 1 order by 1 desc ), Allmetrics as ( select coalesce(fi.Date,MC.date, RS.Return_Date, ID.invoice_date,c.start_date) as date, upper(coalesce(FI.Channel, MC.channel, RS.Channel, ID.Channel,c.c_channel)) as channel, upper(coalesce(FI.Shop_Name, MC.Shop_Name, RS.Shop_name, ID.Shop_name,c_Marketplace)) as Marketplace, upper(trim(coalesce(FI.product, MC.product, RS.product, ID.product,c.product))) as product, upper(coalesce(FI.data_source, MC.data_source, RS.data_source, ID.data_source,c.data_source)) as data_source, upper(coalesce(FI.category, MC.category, RS.category, ID.category,c.category)) as category, consultation_completed, consultation_confirmed, cod_orders, prepaid_orders, Total_Sales, wo_rr_TOTAL_Sales, Total_Sales_Ex_Tax, total_consultations, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, Orders_EXCL_CANCL_RTO, New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, RS.TOTAL_RETURNED_QUANTITY as Return_Quantity, RS.TOTAL_RETURN_AMOUNT_EXCL_TAX as Return_Value, Cancelled_Orders, return_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, ifnull(ID.Realised_Revenue,0)- ifnull(RS.TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as Realised_Revenue, ifnull(ID.Realised_Revenue,0) Invoice_Amount_Excl_Tax, IFNULL(pre_invoice_Delivered_Orders,0) - ifnull(return_orders_count,0) as invoice_Delivered_Orders, spend as marketing_spend, RR_SPENDS, wo_RR_SPENDS, Repeat_Customer_Revenue, Total_RR_Orders, Total_wo_RR_Orders, RR_Booked_Rev_After_Tax, wo_RR_Booked_Rev_After_Tax, ifnull(wo_rr_pre_invoice_Delivered_Orders,0) - ifnull(wo_rr_return_orders_count,0) as wo_rr_invoice_Delivered_Orders, ifnull(ID.wo_rr_Realised_Revenue,0)- ifnull(RS.wo_rr_TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as wo_rr_Realised_Revenue, ifnull(rr_pre_invoice_Delivered_Orders,0) - ifnull(rr_return_orders_count,0) as rr_invoice_Delivered_Orders, ifnull(ID.rr_Realised_Revenue,0)- ifnull(RS.rr_TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as rr_Realised_Revenue, from orders FI full outer join spend MC on FI.Date = MC.date and lower(FI.Channel) = lower(MC.channel) and lower(FI.Shop_name)=lower(MC.Shop_name) and lower(FI.product)=lower(MC.product) and lower(FI.data_source)=lower(MC.data_source) and lower(FI.category)=lower(MC.category) full outer join returnsales RS on RS.return_date = coalesce(FI.Date,MC.Date) and lower(RS.Channel) = lower(coalesce(FI.Channel,MC.Channel)) and lower(RS.Shop_name)=lower(coalesce(FI.Shop_name, MC.Shop_name)) and lower(RS.product)=lower(coalesce(FI.product, MC.product)) and lower(RS.data_source)=lower(coalesce(FI.data_source, MC.data_source)) and lower(RS.category)=lower(coalesce(FI.category, MC.category)) full outer join invoicedatemetrics ID on ID.invoice_date = coalesce(FI.Date,MC.Date, RS.return_date) and lower(coalesce(FI.Channel,MC.Channel,RS.Channel))=lower(ID.Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name, RS.Shop_name))=lower(ID.Shop_name) and lower(coalesce(FI.product, MC.product, RS.product))=lower(ID.product) and lower(coalesce(FI.data_source, MC.data_source, RS.data_source))=lower(ID.data_source) and lower(coalesce(FI.category, MC.category, RS.category))=lower(ID.category) full outer join consultations c on c.start_date= coalesce(FI.Date,MC.Date, RS.return_date,id.invoice_date) and lower(coalesce(FI.Channel,MC.Channel,RS.Channel,id.Channel))=lower(c.c_Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name, RS.Shop_name,id.Shop_name))=lower(c.c_marketplace) and lower(coalesce(FI.product, MC.product, RS.product,id.product))=lower(c.product) and lower(coalesce(FI.data_source, MC.data_source, RS.data_source,id.data_source))=lower(c.data_source) and lower(coalesce(FI.category, MC.category, RS.category,id.category))=lower(c.category) ) select AM.Date as date, AM.channel channel, AM.Marketplace as Marketplace, AM.product as product, AM.category as category, AM.data_source, sum(consultation_completed)consultation_completed, sum(consultation_confirmed)consultation_confirmed, sum(Total_Sales) Total_Sales, sum(wo_rr_TOTAL_Sales) wo_rr_TOTAL_Sales, sum(Total_Sales_Ex_Tax) Total_Sales_Ex_Tax, sum(total_consultations) as total_consultations, sum(cod_Orders) as cod_Orders, sum(prepaid_Orders) as prepaid_Orders, sum(TOTAL_SALES_EXCL_CANCL) TOTAL_SALES_EXCL_CANCL, sum(TOTAL_SALES_EXCL_CANCL_RTO) TOTAL_SALES_EXCL_CANCL_RTO, sum(Total_Orders) Total_Orders, sum(Orders_EXCL_CANCL) Orders_EXCL_CANCL, sum(Orders_EXCL_CANCL_RTO) Orders_EXCL_CANCL_RTO, sum(New_Customer_Orders) New_Customer_Orders, sum(New_Customer_Orders_EXCL_CANCL) New_Customer_Orders_EXCL_CANCL, sum(Total_New_Customers) Total_New_Customers, sum(New_Customers_EXCL_CANCL) New_Customers_EXCL_CANCL, sum(TOTAL_Unique_Customers) TOTAL_Unique_Customers, sum(Unique_Customers_EXCL_CANCL) Unique_Customers_EXCL_CANCL, sum(Repeat_Customers) Repeat_Customers, sum(Repeat_Customers_EXCL_CANCL) Repeat_Customers_EXCL_CANCL, sum(Repeat_orders) Repeat_orders, sum(Repeat_Revenue)Repeat_Revenue, sum(Repeat_orders_EXCL_CANCL) Repeat_orders_EXCL_CANCL, sum(TOTAL_DISCOUNT) TOTAL_DISCOUNT, sum(TOTAL_DISCOUNT_EXCL_CANCL) TOTAL_DISCOUNT_EXCL_CANCL, sum(TOTAL_TAX) TOTAL_TAX, sum(TAX_EXCL_CANCL) TAX_EXCL_CANCL, sum(TOTAL_SHIPPING_PRICE) TOTAL_SHIPPING_PRICE, sum(SHIPPING_PRICE_EXCL_CANCL) SHIPPING_PRICE_EXCL_CANCL, sum(New_Customer_DISCOUNT) New_Customer_DISCOUNT, sum(New_Customer_Discount_EXCL_CANCL) New_Customer_Discount_EXCL_CANCL, sum(TOTAL_QUANTITY) TOTAL_QUANTITY, sum(QUANTITY_EXCL_CANCL) QUANTITY_EXCL_CANCL, sum(Return_Quantity) Return_Quantity, sum(Return_Value) Return_Value, sum(Cancelled_Orders) Cancelled_Orders, sum(return_orders) return_orders, sum(Net_Orders) Net_Orders, sum(Delivered_Orders) Delivered_Orders, sum(Delivered_Revenue) Delivered_Revenue, sum(Dispatched_Orders) Dispatched_Orders, sum(Dispatched_Revenue) Dispatched_Revenue, sum(Realised_Orders) Realised_Orders, sum(Realised_Revenue) Realised_Revenue, sum(Invoice_Amount_Excl_Tax) Invoice_Amount_Excl_Tax, sum(marketing_spend) marketing_spend, sum(marketing_spend) spend, SUM(RR_SPENDS) RR_SPENDS, SUM(wo_RR_SPENDS) wo_RR_SPENDS, sum(Repeat_Customer_Revenue) as Repeat_Customer_Revenue, sum(invoice_delivered_Orders) as invoice_delivered_Orders, sum(Total_RR_Orders) Total_RR_Orders, sum(Total_wo_RR_Orders) Total_wo_RR_Orders, sum(RR_Booked_Rev_After_Tax) RR_Booked_Rev_After_Tax, sum(wo_RR_Booked_Rev_After_Tax) wo_RR_Booked_Rev_After_Tax, sum(rr_Realised_Revenue) rr_Realised_Revenue, sum(wo_rr_Realised_Revenue) wo_rr_Realised_Revenue, sum(rr_invoice_Delivered_Orders) rr_invoice_Delivered_Orders, sum(wo_rr_invoice_Delivered_Orders) wo_rr_invoice_Delivered_Orders from allmetrics AM group by 1,2,3,4,5,6 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            