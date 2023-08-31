{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.maplemonk.SkinQ_PandL as select SALEORDERITEMCODE ,a.source as Marketing_Source ,a.channel as Marketing_Channel ,a.order_date as Date ,a.order_id ,a.reference_code ,a.shop_name ,a.new_customer_flag ,a.customer_id_final ,payment_mode ,payment_gateway ,a.awb ,b.shipment_aggregator ,b.status shipment_partner_Status ,a.order_Status ,a.shipping_status ,upper(coalesce(final_shipping_status,order_status)) final_status ,a.sku ,a.sku_code ,a.product_name_final PRODUCT_NAME_FINAL ,a.product_sub_category product_sub_category ,a.product_category PRODUCT_CATEGORY ,a.sku_code_child ,a.child_product_name ,a.child_product_category ,a.child_product_subcategory ,a.quantity ,d.MRP*quantity MRP ,a.selling_price Gross_sale ,case when lower(coalesce(final_status,\'1\')) not in (\'cancelled\',\'rto\') then d.cogs*quantity else 0 end as COGS ,a.quantity_child ,d1.MRP*quantity_child MRP_Child ,case when lower(coalesce(final_status,\'1\')) not in (\'cancelled\',\'rto\') then d1.cogs*quantity_child else 0 end as COGS_Child ,case when lower(final_status) = \'cancelled\' then 0 else div0( case when lower(type) like \'%flat fee for each order%\' then c.packaging_charges when lower(type) like \'%percent of sale%\' then c.packaging_charges*a.selling_price end, count(1) over (partition by a.order_id)) end as packaging_cost ,div0(case when lower(final_status) in (\'cancelled\',\'pending to dispatch\',\'pending to process\') then 0 else ifnull(b.forward_shipping_charges,0) end,count(1) over (partition by order_id)) Logistics ,div0(ifnull(b.return_shipping_charges,0),count(1) over (partition by order_id)) as Return_Charges ,div0((case when lower(final_status) in (\'rto\',\'cancelled\') then 0 else b.cod_charges end ) ,count(1) over (partition by order_id) ) Cash_Collection_Charges ,div0(ifnull(e.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Google ,div0(ifnull(f.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Facebook ,div0(ifnull(g.spend,0), count(1) over (partition by a.order_Date::date, a.channel)) as Paid_Marketing_Amazon ,case when lower(h.commission_type) like \'%flat fee for month%\' then div0(ifnull(h.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(h.commission_type) like \'%flat fee for each order%\' then div0(ifnull(h.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(h.commission_type) like \'%percent%\' then div0(ifnull(h.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id)) end as Paid_Marketing_Marketplace ,case when lower(i.commission_type) like \'%flat fee for month%\' then div0(ifnull(i.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(i.commission_type) like \'%flat fee for each order%\' then div0(ifnull(i.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(i.commission_type) like \'%percent%\' then div0(ifnull(i.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id)) end as Marketplace_Agency_Fee ,case when lower(j.commission_type) like \'%flat fee for month%\' then div0(ifnull(j.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(j.commission_type) like \'%flat fee for each order%\' then div0(ifnull(j.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(j.commission_type) like \'%percent%\' then div0(ifnull(j.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Account_Manager_Spend ,case when lower(k.commission_type) like \'%flat fee for month%\' then div0(ifnull(k.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(k.commission_type) like \'%flat fee for each order%\' then div0(ifnull(k.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(k.commission_type) like \'%percent%\' then div0(ifnull(k.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Sampling_Spend ,case when lower(l.commission_type) like \'%flat fee for month%\' then div0(ifnull(l.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(l.commission_type) like \'%flat fee for each order%\' then div0(ifnull(l.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(l.commission_type) like \'%percent%\' then div0(ifnull(l.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Events_Spend ,case when lower(m.commission_type) like \'%flat fee for month%\' then div0(ifnull(m.marketing_spend,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(m.commission_type) like \'%flat fee for each order%\' then div0(ifnull(m.marketing_spend,0), count(1) over (partition by a.order_id)) when lower(m.commission_type) like \'%percent%\' then div0(ifnull(m.marketing_spend/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Events_Material_Spend ,case when lower(n.commission_type) like \'%flat fee for month%\' then div0(ifnull(n.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(n.commission_type) like \'%flat fee for each order%\' then div0(ifnull(n.commission_value,0), count(1) over (partition by a.order_id)) when lower(n.commission_type) like \'%percent%\' then div0(ifnull(n.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Marketplace_commission_percentfee ,case when lower(o.commission_type) like \'%flat fee for month%\' then div0(ifnull(o.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(o.commission_type) like \'%flat fee for each order%\' then div0(ifnull(o.commission_value,0), count(1) over (partition by a.order_id)) when lower(o.commission_type) like \'%percent%\' then div0(ifnull(o.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as Marketplace_commission_flatfee ,case when lower(p.commission_type) like \'%flat fee for month%\' then div0(ifnull(p.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(p.commission_type) like \'%flat fee for each order%\' then div0(ifnull(p.commission_value,0), count(1) over (partition by a.order_id)) when lower(p.commission_type) like \'%percent%\' then div0(ifnull(p.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as CONVERSION_LAB_FEE ,case when lower(q.commission_type) like \'%flat fee for month%\' then div0(ifnull(q.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(q.commission_type) like \'%flat fee for each order%\' then div0(ifnull(q.commission_value,0), count(1) over (partition by a.order_id)) when lower(q.commission_type) like \'%percent%\' then div0(ifnull(q.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as DIGITAL_ROI_FEE ,case when lower(r.commission_type) like \'%flat fee for month%\' then div0(ifnull(r.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(r.commission_type) like \'%flat fee for each order%\' then div0(ifnull(r.commission_value,0), count(1) over (partition by a.order_id)) when lower(r.commission_type) like \'%percent%\' then div0(ifnull(r.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as POOJA_FEE ,case when lower(s.commission_type) like \'%flat fee for month%\' then div0(ifnull(s.commission_value,0), count(1) over (partition by a.source, month(a.order_Date), year(a.order_Date))) when lower(s.commission_type) like \'%flat fee for each order%\' then div0(ifnull(s.commission_value,0), count(1) over (partition by a.order_id)) when lower(s.commission_type) like \'%percent%\' then div0(ifnull(s.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as CONTLO_FEE ,case when lower(t.commission_type) like \'%flat fee for month%\' then div0(ifnull(t.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(t.commission_type) like \'%flat fee for each order%\' then div0(ifnull(t.commission_value,0), count(1) over (partition by a.order_id)) when lower(t.commission_type) like \'%percent%\' then div0(ifnull(t.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as INFLUENCER_FEE ,case when lower(u.commission_type) like \'%flat fee for month%\' then div0(ifnull(u.commission_value,0), count(1) over (partition by a.marketplace, month(a.order_Date), year(a.order_Date))) when lower(u.commission_type) like \'%flat fee for each order%\' then div0(ifnull(u.commission_value,0), count(1) over (partition by a.order_id)) when lower(u.commission_type) like \'%percent%\' then div0(ifnull(u.commission_value/100,0)*a.selling_price, count(1) over (partition by a.order_id))end as GOKWIK_FEE from skinq_db.maplemonk.skinq_db_sales_consolidated a left join (select * from (select shipment_aggregator ,awb,status ,forward_shipping_charges , COD_Charges , return_shipping_charges , row_number() over (partition by awb order by updated_date desc) rw from skinq_db.maplemonk.Skinq_logistics_fact_items ) where rw=1 ) b on a.awb = b.awb left join (select marketplace ,try_to_date(from_date,\'DD-MON-YY\') from_date ,try_to_date(to_date,\'DD-MON-YY\') to_date ,type ,try_to_double(replace(charges,\'%\',\'\'))packaging_charges from skinq_db.maplemonk.mapping_packaging_fee ) c on to_date(a.order_date)::date >= from_date and to_date(a.order_date)::date <= to_date and lower(case when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' end) = lower(c.marketplace) left join (select * from (select sku_code , try_to_date(start_date,\'DD-MON-YY\') start_Date , try_to_date(end_date,\'DD-MON-YY\') End_date , try_to_double(mrp) mrp , try_to_double(cogs) cogs , row_number() over (partition by sku_code, start_date, end_date order by mrp desc) rw from skinq_db.maplemonk.MAPPING_SKU_MRP_COGS ) where rw=1 ) d on replace(d.sku_code,\' \',\'\') = replace(a.sku_code,\' \',\'\') and to_date(a.order_date)::date >= d.start_date and to_date(a.order_date)::date <= d.end_date left join (select * from (select sku_code , try_to_date(start_date,\'DD-MON-YY\') start_Date , try_to_date(end_date,\'DD-MON-YY\') End_date , try_to_double(mrp) mrp , try_to_double(cogs) cogs , row_number() over (partition by sku_code, start_date, end_date order by mrp desc) rw from skinq_db.maplemonk.MAPPING_SKU_MRP_COGS ) where rw=1 ) d1 on replace(d1.sku_code,\' \',\'\') = replace(a.sku_code_child,\' \',\'\') and to_date(a.order_date)::date >= d1.start_date and to_date(a.order_date)::date <= d1.end_date left join (select date, sum(spend) spend from skinq_db.maplemonk.skinq_db_MARKETING_CONSOLIDATED where lower(channel) like \'%google%\' group by date) e on e.date = a.order_Date::date and lower(case when lower(a.channel) like \'%paid google%\' then \'google\' end) like \'%google%\' left join (select date, sum(spend) spend from skinq_db.maplemonk.skinq_db_MARKETING_CONSOLIDATED where lower(channel) like \'%facebook%\' group by date) f on f.date = a.order_Date::date and lower(case when lower(a.channel) like \'%paid social%\' then \'facebook\' end) like \'%facebook%\' left join (select date, sum(spend) spend from skinq_db.maplemonk.skinq_db_MARKETING_CONSOLIDATED where lower(channel) like \'%amazon%\' group by date) g on g.date = a.order_Date::date and lower(a.channel) like \'%amazon%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_marketplace_marketing_spend ) h on h.start_date::date <= a.order_date::date and h.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(h.marketplace) and lower(h.category) like \'%ad spend%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_marketplace_marketing_spend ) i on i.start_date::date <= a.order_date::date and i.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(i.marketplace) and lower(i.category) like \'%agency%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_marketplace_marketing_spend ) j on j.start_date::date <= a.order_date::date and j.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(j.marketplace) and lower(j.category) like \'%account manager%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_marketplace_marketing_spend ) k on k.start_date::date <= a.order_date::date and k.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(k.marketplace) and lower(k.category) like \'%sampl%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_offline_marketing_costs ) l on l.start_date::date <= a.order_date::date and l.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(l.marketplace) and lower(l.category) like \'%event%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(category) category , upper(type) COMMISSION_TYPE , try_to_double(replace(spend,\'%\',\'\')) marketing_spend from skinq_db.maplemonk.mapping_offline_marketing_costs ) m on m.start_date::date <= a.order_date::date and m.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(m.marketplace) and lower(m.category) like \'%material%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(commission_value,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_marketplace_commissions ) n on n.start_date::date <= a.order_date::date and n.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(n.marketplace) and lower(n.COMMISSION_TYPE) like \'%percent%\' left join (select try_to_date(start_date,\'DD-MON-YY\') start_date , try_to_date(end_date,\'DD-MON-YY\') end_date , upper(marketplace) marketplace , upper(commission_type) COMMISSION_TYPE , try_to_double(replace(commission_value,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_marketplace_commissions ) o on o.start_date::date <= a.order_date::date and o.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(o.marketplace) and lower(o.COMMISSION_TYPE) like \'%flat fee for each order%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date , upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_shopify_and_tool_costs ) p on p.start_date::date <= a.order_date::date and p.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(p.marketplace) and lower(a.channel) like \'%paid social%\' and lower(p.partner) like \'%conversion lab%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date ,upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_shopify_and_tool_costs ) q on q.start_date::date <= a.order_date::date and q.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(q.marketplace) and lower(q.partner) like \'%digital roi%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date ,upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_shopify_and_tool_costs ) r on r.start_date::date <= a.order_date::date and r.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(r.marketplace) and lower(r.partner) like \'%pooja saini%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date ,upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_shopify_and_tool_costs ) s on s.start_date::date <= a.order_date::date and s.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(s.marketplace) and lower(a.source) like \'%contlo%\' and lower(s.partner) like \'%contlo%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date ,upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_shopify_and_tool_costs ) t on t.start_date::date <= a.order_date::date and t.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(t.marketplace) and lower(t.partner) like \'%influencer%\' left join (select try_to_date(from_date,\'DD-MON-YY\') start_date , try_to_date(to_date,\'DD-MON-YY\') end_date ,upper(partner) Partner , upper(marketplace) marketplace , upper(detail) COMMISSION_TYPE , try_to_double(replace(charges,\'%\',\'\')) commission_value from skinq_db.maplemonk.mapping_payment_costs ) u on u.start_date::date <= a.order_date::date and u.end_date::date >=a.order_date::date and lower(case when lower(a.marketplace) like \'%flipkart%\' then \'FLIPKART\' when lower(a.marketplace) like \'%nykaa%\' then \'NYKAA\' when lower(a.marketplace) like \'%shopify%\' then \'SHOPIFY\' else a.marketplace end) = lower(u.marketplace) and lower(u.partner) like \'%gokwik%\' and lower(a.payment_gateway) like \'%gokwik%\' ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        