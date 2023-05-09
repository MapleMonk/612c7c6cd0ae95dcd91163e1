{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.PandL_xyxx as select a.source as Marketing_Channel ,a.order_date as Date ,a.selling_price Gross_sale ,d.MRP*quantity MRP ,case when lower(a.final_status) in (\'delivered\',\'in-transit\',\'pending to dispatch\', \'pending to process\',\'initiated\') then d.cogs*quantity else 0 end as COGS ,a.order_id ,a.sku , a.shop_name , a.awb ,a.quantity ,a.shippingpackagecode , b.status shipment_partner_Status , a.order_Status , a.unicommerce_shipping_status ,a.final_status , payment_method ,coalesce(d.product_sub_category,a.product_category) product_sub_category ,b.shipment_aggregator ,a.order_name ,div0(case when lower(a.final_status) in (\'canceled\',\'pending to dispatch\',\'pending to process\') then 0 else coalesce(b.shipping_charges,e.shipping_charges) end,count(1) over (partition by order_id)) Logistics ,div0(case when lower(a.final_status) = \'rto\' then Logistics else 0 end,count(1) over (partition by order_id)) as Return_Charges ,case when lower(a.final_status) = \'canceled\' then 0 else ( case when sum(case when lower(coalesce(d.product_sub_category,a.product_category)) in (\'trunk\',\'brief\') then a.quantity else 0 end) over (partition by a.order_id) <= 5 and sum(case when lower(coalesce(d.product_sub_category,a.product_category)) in (\'trunk\',\'brief\') then 1 else 0 end) over (partition by a.order_id) > 0 then 8 when sum(case when lower(coalesce(d.product_sub_category,a.product_category)) in (\'trunk\',\'brief\') then a.quantity else 0 end) over (partition by a.order_id) > 5 then 23 else 29 end) / count(1) over (partition by a.order_id) end as packaging_cost ,div0(coalesce(b.cod_charges,case when lower(a.payment_method) = \'cod\' then e.cod_charges end),count(1) over (partition by order_id)) Cash_Collection_Charges ,div0(f.fee, count(1) over (partition by order_id)) as Transaction_Fee ,div0(g.spend, count(1) over (partition by a.order_Date)) as Paid_Marketing_Google ,div0(h.spend, count(1) over (partition by a.order_Date)) as Paid_Marketing_Facebook ,div0(i.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Paid_Marketing_CRED ,div0(j.charges, count(1) over (partition by month(a.order_Date), year(a.order_date))) as Paid_Marketing_Affiliates ,div0(k.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Paid_Marketing_Bing ,a.selling_price*(replace(l.charges,\'%\',\'\')::float)/100 Shopify_transaction_fee ,div0(m.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Shopify_hosting_fee ,div0(n.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Semrush ,div0(o.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as LaterGram ,div0(p.total_cost, count(1) over (partition by a.order_Date)) as SMS_partner ,div0(q.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Contlo ,div0(r.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as Telephone ,div0(s.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as AgoraPulse ,div0(t.charges, count(1) over (partition by month(a.order_Date), year(a.order_Date))) as MailChimp from XYXX_DB.maplemonk.sales_consolidated_XYXX a left join (select * from (select shipment_aggregator,awb,status,Shipping_Charges, COD_Charges, row_number() over (partition by awb order by updated_date desc) rw from xyxx_db.maplemonk.logistics_fact_items_xyxx) where rw=1 ) b on a.awb = b.awb left join (select * from (select sku_id, start_date, end_date, product_sub_category, product_category, product_name, mrp, cogs, row_number() over (partition by sku_id, start_date, end_date order by mrp desc) rw from xyxx_db.maplemonk.mapping_product_mrp_cogs) where rw=1) d on d.sku_id = a.sku and to_date(a.order_date)::date >= to_date(start_date) and to_date(a.order_date)::date <= to_date(end_date) left join xyxx_db.maplemonk.mapping_shipment_cost e on e.from_date::date <= a.order_date::date and e.to_date::date >=a.order_date::date left join xyxx_db.maplemonk.mapping_order_razorpay_fee f on a.order_name = f.order_receipt left join (select date, sum(spend) spend from xyxx_db.maplemonk.marketing_consolidated_xyxx where channel = \'Google\' group by date) g on g.date = a.order_Date::date left join (select date, sum(spend) spend from xyxx_db.maplemonk.marketing_consolidated_xyxx where channel = \'Facebook\' group by date) h on h.date = a.order_Date::date left join (select charges::float charges, partner, to_Date,from_date from xyxx_db.maplemonk.mapping_marketing_costs where partner = \'CRED\') i on a.order_Date::date >= to_date(i.from_date) and a.order_Date::date <= to_Date(i.to_Date) left join (select charges::float charges, partner, to_Date,from_date from xyxx_db.maplemonk.mapping_marketing_costs where partner = \'Affiliates\') j on a.order_Date::date >= to_Date(j.from_date) and a.order_Date::date <= to_date(j.to_Date) left join (select charges::float charges, partner, to_Date,from_date from xyxx_db.maplemonk.mapping_marketing_costs where partner = \'Bing\') k on a.order_Date::date >= to_Date(k.from_date) and a.order_Date::date <= to_date(k.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'Shopify\' and type = \'Transaction Fee\') l on a.order_Date::date >= to_Date(l.from_date) and a.order_Date::date <= to_date(l.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'Shopify\' and type = \'Hosting Fee\') m on a.order_Date::date >= to_Date(m.from_date) and a.order_Date::date <= to_date(m.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'SemRush\' ) n on a.order_Date::date >= to_Date(n.from_date) and a.order_Date::date <= to_date(n.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'LaterGram\' ) o on a.order_Date::date >= to_Date(o.from_date) and a.order_Date::date <= to_date(o.to_Date) left join (select sent_date, sum(total_cost) total_cost from xyxx_db.maplemonk.contlo_fact_items_xyxx group by 1) p on a.order_Date::date = to_Date(p.sent_date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'Contlo\' ) q on a.order_Date::date >= to_Date(q.from_date) and a.order_Date::date <= to_date(q.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'Telephone\' ) r on a.order_Date::date >= to_Date(r.from_date) and a.order_Date::date <= to_date(r.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'AgoraPulse\' ) s on a.order_Date::date >= to_Date(s.from_date) and a.order_Date::date <= to_date(s.to_Date) left join (select * from xyxx_db.maplemonk.mapping_tool_costs where partner = \'MailChimp\' ) t on a.order_Date::date >= to_Date(t.from_date) and a.order_Date::date <= to_date(t.to_Date) where lower(a.shop_name) in (\'shopify_india\', \'cred\');",
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
                        