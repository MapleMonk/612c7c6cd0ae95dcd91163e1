{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table RPSG_DB.MAPLEMONK.Sales_Cost_Source_Three60you_intermediate as with invoicedatemetrics as ( select try_to_date(FI.invoice_date) Invoice_Date ,upper(data_source) as data_source ,upper(FI.final_channel) Channel ,upper(FI.SHOP_NAME) SHOP_NAME ,count(distinct case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then order_id end ) rr_pre_invoice_Delivered_Orders ,sum(case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then ifnull(selling_price,0) - ifnull(tax,0) end ) rr_Realised_Revenue ,count(distinct case when lower(order_status) not in (\'cancelled\') then order_id end ) pre_invoice_Delivered_Orders ,sum(ifnull((case when lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0)) Realised_Revenue ,sum(ifnull((case when lower(order_status) not in (\'cancelled\') and lower(new_customer_flag) = \'repeat\' then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0)) repeat_Realised_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI where not(lower(order_status) in (\'cancelled\')) and invoice_date != \'\' group by 1,2,3,4 order by 1 desc ), returnsales as ( select return_date::date return_date ,upper(channel) AS channel ,upper(data_source) as data_source ,shop_name ,count(distinct case when rr_flag = 1 then reference_code end ) rr_return_orders_count ,sum(case when lower(new_customer_flag) = \'repeat\' then total_return_amount_excl_tax else 0 end) repeat_return_amount_excl_tax ,sum( case when rr_flag = 1 then ifnull(total_return_amount_excl_tax,0) end) rr_TOTAL_RETURN_AMOUNT_EXCL_TAX ,count(distinct reference_code) as return_orders_count ,sum(total_return_amount) TOTAL_RETURN_AMOUNT ,sum(total_return_amount_excl_tax) TOTAL_RETURN_AMOUNT_EXCL_TAX ,sum(total_returned_quantity) TOTAL_RETURNED_QUANTITY from RPSG_DB.MAPLEMONK.easyecom_returns_summary_three60 where lower(company_name) like any (\'%herbolab%\',\'%dr vaidya%\') group by 1,2,3,4 order by 1 desc ), orders as ( select FI.order_date::date Date ,upper(FI.final_channel) Channel ,upper(FI.SHOP_NAME) SHOP_NAME ,upper(data_source) as data_source ,count(distinct case when lower(payment_mode) = \'cod\' then order_id end) cod_Orders ,count(distinct case when lower(payment_mode) = \'prepaid\' then order_id end) prepaid_Orders ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales ,count(distinct appointment_id) as total_consultations ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL ,ifnull(sum(case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\')) then FI.SELLING_PRICE end),0) TOTAL_SALES_EXCL_CANCL_RTO ,ifnull(sum(case when lower(final_status) like \'%cancel%\' then ifnull(FI.SELLING_PRICE,0) - ifnull(tax,0) end),0) TOTAL_CANCL_SALES_EXCL_TAX ,count(distinct case when not(lower(final_status) like any (\'%return%\',\'%rto%\',\'%cancel%\'))then order_id end ) as Orders_EXCL_CANCL_RTO ,sum(ifnull(selling_price,0)) - sum(ifnull(tax,0)) as Total_Sales_Ex_Tax ,count(distinct FI.order_id) Total_Orders ,count(distinct case when rr_flag = 1 then FI.order_id end ) Total_RR_Orders ,sum(case when rr_flag = 1 then ifnull(selling_price,0) - ifnull(tax,0) end ) RR_Booked_Rev_After_Tax ,count(distinct FI.order_id) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.order_id end) Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) as marketplace_New_Customer_Orders ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') then FI.order_id end)) as New_Customer_Orders_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) as Total_New_Customers ,count(distinct(case when lower(FI.new_customer_flag) = \'new\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'new\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end)) New_Customers_EXCL_CANCL ,count(distinct FI.customer_id_final) as TOTAL_Unique_Customers ,(count(distinct FI.customer_id_final) - count(distinct case when lower(FI.order_status) in (\'cancelled\') then FI.customer_id_final end)) as Unique_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) as Repeat_Customers ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.customer_id_final end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.customer_id_final end))) Repeat_Customers_EXCL_CANCL ,count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then FI.order_id end)) as Repeat_Orders ,sum(case when lower(FI.new_customer_flag) = \'repeat\' then ifnull(FI.selling_price,0) end) as Repeat_Revenue ,(count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' then Fi.order_id end)) - count(distinct(case when lower(FI.new_customer_flag) = \'repeat\' and lower(FI.order_status) in (\'cancelled\') and FI.return_flag = 0 then FI.order_id end))) Repeat_orders_EXCL_CANCL ,ifnull(sum(FI.discount_mrp),0) TOTAL_DISCOUNT ,(ifnull(sum(FI.discount_mrp),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.discount_mrp end),0)) TOTAL_DISCOUNT_EXCL_CANCL ,ifnull(sum(FI.tax),0) TOTAL_TAX ,(ifnull(sum(FI.tax),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.tax end),0)) TAX_EXCL_CANCL ,ifnull(sum(FI.shipping_price),0) TOTAL_SHIPPING_PRICE ,(ifnull(sum(FI.shipping_price),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.shipping_price end),0)) SHIPPING_PRICE_EXCL_CANCL ,ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) as New_Customer_Discount ,(ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' then FI.discount_mrp end),0) - ifnull(sum(case when lower(FI.new_customer_flag) = \'new\' and lower(order_status) in (\'cancelled\') then FI.discount_mrp end),0)) as New_Customer_Discount_EXCL_CANCL ,ifnull(sum(FI.suborder_quantity),0) TOTAL_QUANTITY ,(ifnull(sum(FI.suborder_quantity),0) - ifnull(sum(case when lower(FI.order_status) in (\'cancelled\') then FI.suborder_quantity end),0)) QUANTITY_EXCL_CANCL ,ifnull(sum(case when FI.return_flag=1 then FI.suborder_quantity end),0) as Return_Quantity ,ifnull(sum(case when FI.return_flag=1 then ifnull(FI.SELLING_PRICE,0) end),0) as Return_Value ,count(distinct case when FI.return_flag=1 then order_id end )as Return_Orders ,count(distinct case when lower(final_status) in (\'cancelled\') then order_id end) Cancelled_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') and return_flag=0 then order_id end) Net_Orders ,count(distinct case when lower(final_status) in (\'delivered\') then order_id end) Delivered_Orders ,count(distinct case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then order_id end) Returned_Orders ,count(distinct case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then order_id end) Dispatched_Orders ,Orders_EXCL_CANCL - Returned_Orders as Realised_Orders ,ifnull(sum(case when lower(final_status) in (\'delivered\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Delivered_Revenue ,ifnull(sum(case when lower(final_status) in (\'returned\',\'rto\') or lower(final_status) in (\'returned\',\'rto\') then ifnull(FI.SELLING_PRICE,0)-ifnull(FI.TAX,0) end),0) Returned_Revenue ,ifnull(sum(case when manifest_date is not null and lower(order_status) not in (\'cancelled\') then ifnull(FI.SELLING_PRICE,0) end),0) Dispatched_Revenue ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L3M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-6,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L6M_Customers_Retained ,count(case when date_trunc(\'month\',acquisition_date::date)>=dateadd(month,-3,date_trunc(\'month\',order_date::date)) and date_trunc(\'month\',acquisition_date::date)<date_trunc(\'month\',order_date::date) then customer_id_final end) L12M_Customers_Retained ,sum(case when lower(FI.new_customer_flag_month) = \'repeat\' then ifnull(FI.selling_price,0) end) Repeat_Customer_Revenue from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 FI group by 1,2,3,4 ), spend as ( select Date, channel, Upper(account) as shop_name, upper(data_source) as data_source ,sum(spend)Spend from rpsg_db.MAPLEMONK.marketing_consolidated_three60you group by 1,2,3,4 order by 1 desc ) ,Users as ( select date, shop_name , \'three60\' as data_source, final_channel channel, sum(ifnull(sessions,0))sessions, sum(totalusers) users, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(ga_consultation_booked) as ga_consultation_booked from ( select to_date(date,\'YYYYMMDD\')Date, replace(split(rf.sessionsourcemedium,\'/\')[0],\'\"\',\'\')source, replace(split(rf.sessionsourcemedium,\'/\')[1],\'\"\',\'\')medium, final_channel, rf.sessionsourcemedium, landingpage, sessions, totalusers, addtocarts, checkouts, \"keyEvents:consult_booked\" as ga_consultation_booked, case when lower(landingpage) like any (\'%joint%\', \'%pain%\', \'%plus%\', \'%arthritis%\') then \'Three60plus\' when lower(landingpage) like any (\'%expert%\',\'%appointment%\') then \'CONSULTATIONS\' else \'Three60\' end as shop_name from rpsg_db.maplemonk.ga4_three60you_updated_sessions_checkouts_by_landingpage rf left join ( select * from ( select *, row_number() over(partition by lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')) order by 1)rw, from rpsg_db.maplemonk.three60you_ga_channel_mapping )where rw=1 and (source is not null and medium is not null) )ga_mapping on lower(ga_mapping.SESSIONSOURCEMEDIUM)= lower(rf.SESSIONSOURCEMEDIUM) ) where date >= \'2024-03-20\' group by 1,2,3,4 ), consultations1 as ( SELECT CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', start_timestamp :: DATETIME) ::date AS start_date, \'CONSULTATIONS\' AS c_marketplace, \'three60\' as data_source, null as c_channel, count(distinct case when LOWER(status) = \'completed\' then id end) as consultation_completed, count(distinct id ) as consultation_confirmed FROM ( select * from ( select a.*, case when LOWER(status) = \'completed\' then 1 else 2 end as df, right(regexp_replace(c.phone, \'[^a-zA-Z0-9]+\'),10) phone, row_number() over(partition by end_timestamp::date,phone order by df asc ) rw from rpsg_db.maplemonk.pg_three60you_appointment a left join rpsg_db.maplemonk.pg_three60you_customer c on lower(c.id) = lower(a.customer_id) where right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) not in (select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) from RPSG_DB.maplemonk.three60you_test_consultations ) )where rw=1 ) where start_date < current_date() group by 1 order by 1 desc ), consultations as ( select coalesce(start_date,start_date1) start_date, consultation_completed, consultation_confirmed, \'CONSULTATIONS\' AS c_marketplace, \'three60\' as data_source, null as c_channel, consultation_leads from consultations1 a full outer join ( SELECT CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', created_at :: DATETIME)::date AS start_date1, count(distinct id ) as consultation_leads FROM ( select * from ( select a.*, case when LOWER(status) = \'completed\' then 1 else 2 end as df, right(regexp_replace(c.phone, \'[^a-zA-Z0-9]+\'),10) phone, row_number() over(partition by end_timestamp::date,phone order by df asc ) rw from rpsg_db.maplemonk.pg_three60you_appointment a left join rpsg_db.maplemonk.pg_three60you_customer c on lower(c.id) = lower(a.customer_id) where right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) not in (select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) from RPSG_DB.maplemonk.three60you_test_consultations ) )where rw=1 ) where start_date1 < current_date() group by 1 order by 1 desc )b on a.start_date = b.start_date1 ), Allmetrics as ( select coalesce(fi.Date,MC.date,SC.date, RS.Return_Date, ID.invoice_date,c.start_date) as date, upper(coalesce(FI.Channel, MC.channel, SC.channel, RS.Channel, ID.Channel,c.c_channel)) as channel, upper(coalesce(FI.Shop_Name, MC.Shop_Name, SC.Shop_name, RS.Shop_name, ID.Shop_name,c_Marketplace)) as Marketplace, upper(coalesce(FI.data_source, MC.data_source, SC.data_source, RS.data_source, ID.data_source,c.data_source)) as data_source, consultation_completed, consultation_confirmed, consultation_leads, cod_orders, prepaid_orders, Total_Sales, total_consultations, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, Orders_EXCL_CANCL_RTO, marketplace_New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, RS.TOTAL_RETURNED_QUANTITY as Return_Quantity, RS.TOTAL_RETURN_AMOUNT_EXCL_TAX as Return_Value, Cancelled_Orders, return_Orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, ifnull(ID.Realised_Revenue,0)- ifnull(RS.TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as Realised_Revenue, ifnull(ID.Realised_Revenue,0) Invoice_Amount_Excl_Tax, spend as marketing_spend, SC.Sessions as traffic, SC.users as users, sc.addtocarts as addtocarts, sc.ga_consultation_booked as ga_consultation_booked, sc.checkouts as checkouts, Repeat_Customer_Revenue, ifnull(pre_invoice_Delivered_Orders,0) - ifnull(return_orders_count,0) as invoice_Delivered_Orders, return_orders_count, pre_invoice_Delivered_Orders, TOTAL_CANCL_SALES_EXCL_TAX, Total_Sales_Ex_Tax, Total_RR_Orders, RR_Booked_Rev_After_Tax, ifnull(rr_pre_invoice_Delivered_Orders,0) - ifnull(rr_return_orders_count,0) as rr_invoice_Delivered_Orders, ifnull(ID.rr_Realised_Revenue,0)- ifnull(RS.rr_TOTAL_RETURN_AMOUNT_EXCL_TAX,0) as rr_Realised_Revenue, ifnull(ID.repeat_Realised_Revenue,0)- ifnull(RS.repeat_return_amount_excl_tax,0) as repeat_Realised_Revenue from orders FI full outer join spend MC on FI.Date = MC.date and lower(FI.Channel) = lower(MC.channel) and lower(FI.Shop_name)=lower(MC.Shop_name) and lower(FI.data_source)=lower(MC.data_source) full outer join Users SC on coalesce(FI.Date,MC.Date)=SC.Date and lower(coalesce(FI.Channel,MC.Channel))=lower(SC.Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name))=lower(SC.Shop_name) and lower(coalesce(FI.data_source, MC.data_source))=lower(SC.data_source) full outer join returnsales RS on RS.return_date = coalesce(FI.Date,MC.Date,SC.Date) and lower(RS.Channel) = lower(coalesce(FI.Channel,MC.Channel, SC.Channel)) and lower(RS.Shop_name)=lower(coalesce(FI.Shop_name, MC.Shop_name, SC.Shop_name)) and lower(RS.data_source)=lower(coalesce(FI.data_source, MC.data_source, SC.data_source)) full outer join invoicedatemetrics ID on ID.invoice_date = coalesce(FI.Date,MC.Date,SC.Date, RS.return_date) and lower(coalesce(FI.Channel,MC.Channel, SC.Channel,RS.Channel))=lower(ID.Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name, SC.Shop_name, RS.Shop_name))=lower(ID.Shop_name) and lower(coalesce(FI.data_source, MC.data_source, SC.data_source, RS.data_source))=lower(ID.data_source) full outer join consultations c on c.start_date= coalesce(FI.Date,MC.Date,SC.Date, RS.return_date,id.invoice_date) and lower(coalesce(FI.Channel,MC.Channel, SC.Channel,RS.Channel,id.Channel))=lower(c.c_Channel) and lower(coalesce(FI.Shop_name, MC.Shop_name, SC.Shop_name, RS.Shop_name,id.Shop_name))=lower(c.c_marketplace) and lower(coalesce(FI.data_source, MC.data_source, SC.data_source, RS.data_source,id.data_source))=lower(c.data_source) ) select AM.Date as date, AM.channel channel, AM.Marketplace as Marketplace, AM.data_source as data_source, sum(consultation_completed)consultation_completed, sum(consultation_confirmed)consultation_confirmed, sum(consultation_leads) consultation_leads, sum(Total_Sales) Total_Sales, sum(total_consultations) as total_consultations, sum(cod_Orders) as cod_Orders, sum(prepaid_Orders) as prepaid_Orders, sum(TOTAL_SALES_EXCL_CANCL) TOTAL_SALES_EXCL_CANCL, sum(TOTAL_SALES_EXCL_CANCL_RTO) TOTAL_SALES_EXCL_CANCL_RTO, sum(Total_Orders) Total_Orders, sum(Orders_EXCL_CANCL) Orders_EXCL_CANCL, sum(Orders_EXCL_CANCL_RTO) Orders_EXCL_CANCL_RTO, sum(marketplace_New_Customer_Orders) marketplace_New_Customer_Orders, sum(New_Customer_Orders_EXCL_CANCL) New_Customer_Orders_EXCL_CANCL, sum(Total_New_Customers) Total_New_Customers, sum(New_Customers_EXCL_CANCL) New_Customers_EXCL_CANCL, sum(TOTAL_Unique_Customers) TOTAL_Unique_Customers, sum(Unique_Customers_EXCL_CANCL) Unique_Customers_EXCL_CANCL, sum(Repeat_Customers) Repeat_Customers, sum(Repeat_Customers_EXCL_CANCL) Repeat_Customers_EXCL_CANCL, sum(Repeat_orders) Repeat_orders, sum(Repeat_Revenue)Repeat_Revenue, sum(Repeat_orders_EXCL_CANCL) Repeat_orders_EXCL_CANCL, sum(TOTAL_DISCOUNT) TOTAL_DISCOUNT, sum(TOTAL_DISCOUNT_EXCL_CANCL) TOTAL_DISCOUNT_EXCL_CANCL, sum(TOTAL_TAX) TOTAL_TAX, sum(TAX_EXCL_CANCL) TAX_EXCL_CANCL, sum(TOTAL_SHIPPING_PRICE) TOTAL_SHIPPING_PRICE, sum(SHIPPING_PRICE_EXCL_CANCL) SHIPPING_PRICE_EXCL_CANCL, sum(New_Customer_DISCOUNT) New_Customer_DISCOUNT, sum(New_Customer_Discount_EXCL_CANCL) New_Customer_Discount_EXCL_CANCL, sum(TOTAL_QUANTITY) TOTAL_QUANTITY, sum(QUANTITY_EXCL_CANCL) QUANTITY_EXCL_CANCL, sum(Return_Quantity) Return_Quantity, sum(Return_Value) Return_Value, sum(Cancelled_Orders) Cancelled_Orders, sum(return_orders) return_orders, sum(Net_Orders) Net_Orders, sum(Delivered_Orders) Delivered_Orders, sum(Delivered_Revenue) Delivered_Revenue, sum(Dispatched_Orders) Dispatched_Orders, sum(Dispatched_Revenue) Dispatched_Revenue, sum(Realised_Orders) Realised_Orders, sum(Realised_Revenue) Realised_Revenue, sum(Invoice_Amount_Excl_Tax) Invoice_Amount_Excl_Tax, sum(marketing_spend) marketing_spend, sum(ifnull(Traffic,0)) Traffic, sum(ifnull(users,0)) Users, sum(ifnull(addtocarts,0)) as addtocarts, sum(ifnull(ga_consultation_booked,0)) as ga_consultation_booked, sum(ifnull(checkouts,0)) as checkouts, sum(Repeat_Customer_Revenue) as Repeat_Customer_Revenue, sum(invoice_Delivered_Orders) as invoice_Delivered_Orders, sum(return_orders_count) as return_orders_count, sum(pre_invoice_Delivered_Orders) pre_invoice_Delivered_Orders, sum(TOTAL_CANCL_SALES_EXCL_TAX) TOTAL_CANCL_SALES_EXCL_TAX, sum(Total_Sales_Ex_Tax) Total_Sales_Ex_Tax, sum(Total_RR_Orders) Total_RR_Orders, sum(RR_Booked_Rev_After_Tax) RR_Booked_Rev_After_Tax, sum(rr_Realised_Revenue) rr_Realised_Revenue, sum(rr_invoice_Delivered_Orders) rr_invoice_Delivered_Orders, sum(repeat_Realised_Revenue) as repeat_Realised_Revenue from allmetrics AM group by 1,2,3,4 ; Create or replace table RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_THREE60YOU as select coalesce(a.date, b.date) as date1, upper(coalesce(b.channel, a.channel)) as channel1, upper(coalesce(a.marketplace, b.shop_name)) as Marketplace, upper(coalesce(a.data_source, b.data_source)) as data_source1, consultation_completed, consultation_confirmed, consultation_leads, Total_Sales, total_consultations, return_orders_count, pre_invoice_Delivered_Orders, cod_Orders, prepaid_Orders, Total_Sales_Ex_Tax, TOTAL_SALES_EXCL_CANCL, TOTAL_SALES_EXCL_CANCL_RTO, Orders_EXCL_CANCL_RTO, Total_Orders, Orders_EXCL_CANCL, marketplace_New_Customer_Orders, New_Customer_Orders_EXCL_CANCL, Total_New_Customers, New_Customers_EXCL_CANCL, TOTAL_Unique_Customers, Unique_Customers_EXCL_CANCL, Repeat_Customers, Repeat_Customers_EXCL_CANCL, Repeat_orders, Repeat_Revenue, Repeat_orders_EXCL_CANCL, TOTAL_DISCOUNT, TOTAL_DISCOUNT_EXCL_CANCL, TOTAL_TAX, TAX_EXCL_CANCL, TOTAL_SHIPPING_PRICE, SHIPPING_PRICE_EXCL_CANCL, New_Customer_DISCOUNT, New_Customer_Discount_EXCL_CANCL, TOTAL_QUANTITY, QUANTITY_EXCL_CANCL, Return_Quantity, Return_Value, Cancelled_Orders, return_orders, Net_Orders, Delivered_Orders, Delivered_Revenue, Dispatched_Orders, Dispatched_Revenue, Realised_Orders, Invoice_Amount_Excl_Tax, Realised_Revenue, marketing_spend, Traffic, Users, addtocarts, ga_consultation_booked, checkouts, Repeat_Customer_Revenue, invoice_Delivered_Orders, TOTAL_CANCL_SALES_EXCL_TAX, Total_RR_Orders, RR_Booked_Rev_After_Tax, rr_Realised_Revenue, rr_invoice_Delivered_Orders, ifnull(b.customers,0) as MC_MP_Customer_Till_Date, ifnull(b.gross_sales,0) as MC_MP_Sales_Till_Date, repeat_Realised_Revenue from RPSG_DB.MAPLEMONK.SALES_COST_SOURCE_THREE60YOU_INTERMEDIATE a full outer join (select date ,upper(shop_name) shop_name ,upper(channel) channel ,upper(data_source) data_source ,sum(gross_sales) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) gross_sales ,sum(customers) over (partition by shop_name, channel order by date asc rows between unbounded preceding and current row) customers from ( select B.date ,upper(B.shop_name) shop_name ,upper(B.final_channel) as channel ,upper(B.data_source) as data_source ,sum(ifnull(selling_price,0)) gross_sales ,count(distinct case when new_customer_flag = \'New\' then customer_id_final end) customers from rpsg_db.maplemonk.sales_consolidated_three60 A full outer join (select * from (select distinct order_date::date date from rpsg_db.maplemonk.sales_consolidated_three60 X) cross join (select distinct shop_name, data_source,final_channel from rpsg_db.maplemonk.sales_consolidated_three60) Y) B on A.order_date::date=B.date AND lower(A.SHOP_NAME)=lower(B.SHOP_NAME) AND lower(A.final_channel)=lower(B.final_channel) AND lower(A.data_source)=lower(B.data_source) group by B.date, upper(B.shop_name), upper(B.final_channel),upper(B.data_source) order by B.date desc ) order by date desc ) b on a.Date = b.date and lower(a.Channel) = lower(b.channel) and lower(a.marketplace)=lower(b.Shop_name) and lower(a.data_source)=lower(b.data_source) order by 1 desc ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.Sales_Cost_Source_three60you AS select sc.*, coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date) date, UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)) channel, upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )) data_source, div0(ov_or.overall_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) overall_orders, div0(ov_or.rr_overall_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) rr_overall_Orders, div0(ov_or.overall_Cancel_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) overall_Cancel_Orders, div0(ov_or.New_Customer_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) New_Customer_Orders, div0(ov_dl_or.overall_invoice_Delivered_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) overall_invoice_Delivered_Orders, div0(ov_dl_or.rr_overall_invoice_Delivered_Orders,count(1) over(partition by coalesce(sc.date1,ov_or.date,ov_dl_or.Invoice_Date),UPPER(coalesce(sc.channel1,ov_or.channel,ov_dl_or.channel)),upper(coalesce(sc.data_source1,ov_or.data_source,ov_dl_or.data_source )))) rr_overall_invoice_Delivered_Orders from RPSG_DB.MAPLEMONK.Sales_Cost_Source_three60you sc full outer join ( select order_date::date as date, data_source, final_channel as channel, count(distinct case when lower(order_status) not in (\'cancelled\') then order_id end ) overall_Orders, count(distinct case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then order_id end ) rr_overall_Orders, count(distinct case when lower(final_status) like \'%cancel%\' then order_id end) overall_Cancel_Orders, count(distinct(case when lower(new_customer_flag) = \'new\' then order_id end)) as New_Customer_Orders from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 group by 1,2,3 )ov_or on sc.date1 = ov_or.date and lower(sc.channel1) = lower(ov_or.channel) and lower(sc.data_source1) = lower(ov_or.data_source) full outer join ( select coalesce(Invoice_Date,return_date)Invoice_Date ,coalesce(fi.data_source,rt.data_source)data_source ,coalesce(fi.channel,rt.channel) channel ,ifnull(invoice_Delivered_Orders,0) - ifnull(return_orders_count,0) as overall_invoice_Delivered_Orders ,ifnull(rr_invoice_Delivered_Orders,0) - ifnull(rr_return_orders_count,0) as rr_overall_invoice_Delivered_Orders from ( select try_to_date(invoice_date) Invoice_Date ,data_source ,upper(final_channel) Channel ,count(distinct case when rr_flag = 1 and lower(order_status) not in (\'cancelled\') then order_id end ) as rr_invoice_Delivered_Orders ,count(distinct case when lower(order_status) not in (\'cancelled\') then order_id end ) as invoice_Delivered_Orders from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_THREE60 where not(lower(order_status) in (\'cancelled\') ) and invoice_date != \'\' group by 1,2,3 order by 1 desc )FI full outer join ( select return_date::date return_date ,upper(channel) AS channel ,upper(data_source) as data_source ,count(distinct case when rr_flag = 1 then reference_code end ) rr_return_orders_count ,count(distinct reference_code) as return_orders_count from RPSG_DB.MAPLEMONK.easyecom_returns_summary_three60 where lower(company_name) like any (\'%herbolab%\',\'%dr vaidya%\') group by 1,2,3 order by 1 desc )rt on rt.return_date = FI.Invoice_Date and lower(fi.data_source) = lower(rt.data_source) and lower(fi.channel) = lower(rt.channel) )ov_dl_or on ov_dl_or.Invoice_Date = coalesce(sc.date1,ov_or.date) and lower(ov_dl_or.Channel) = lower(coalesce(sc.channel1,ov_or.channel)) and lower(ov_dl_or.data_source) = lower(coalesce(sc.data_source1,ov_or.data_source)) ;",
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
                        